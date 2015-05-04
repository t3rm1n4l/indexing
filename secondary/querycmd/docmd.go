package querycmd

import "encoding/json"
import "flag"
import "fmt"
import "io"
import "bytes"
import "strings"
import "strconv"
import "net"
import "errors"
import "time"
import "net/http"
import "io/ioutil"
import "os"

import "github.com/couchbase/cbauth"
import "github.com/couchbase/indexing/secondary/logging"
import c "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbase/query/expression"
import "github.com/couchbase/query/parser/n1ql"

// Command object containing parsed result from command-line
// or program constructued list of args.
type Command struct {
	OpType string
	// basic options.
	Server    string
	IndexName string
	Bucket    string
	AdminPort string
	QueryPort string
	Auth      string
	// options for create-index.
	Using     string
	ExprType  string
	PartnStr  string
	WhereStr  string
	SecStrs   []string
	IsPrimary bool
	With      string
	WithPlan  map[string]interface{}
	// options for build index
	Bindexes []string
	// options for Range, Statistics, Count
	Low       c.SecondaryKey
	High      c.SecondaryKey
	Equal     c.SecondaryKey
	Inclusion qclient.Inclusion
	Limit     int64
	// Configuration
	ConfigKey string
	ConfigVal string
	Help      bool
}

// ParseArgs into Command object, return the list of arguments,
// flagset used for parseing and error if any.
func ParseArgs(arguments []string) (*Command, []string, *flag.FlagSet, error) {
	var fields, bindexes string
	var inclusion uint
	var equal, low, high string

	cmdOptions := &Command{}
	fset := flag.NewFlagSet("cmd", flag.ExitOnError)

	// basic options
	fset.StringVar(&cmdOptions.Server, "server", "•", "Cluster server address")
	fset.StringVar(&cmdOptions.Auth, "auth", "•", "Auth user and password")
	fset.StringVar(&cmdOptions.Bucket, "bucket", "", "Bucket name")
	fset.StringVar(&cmdOptions.OpType, "type", "", "Command: scan|stats|scanAll|count|nodes|create|build|drop|list|config")
	fset.StringVar(&cmdOptions.IndexName, "index", "", "Index name")
	// options for create-index
	fset.StringVar(&cmdOptions.WhereStr, "where", "", "where clause for create index")
	fset.StringVar(&fields, "fields", "", "Comma separated on-index fields") // secStrs
	fset.BoolVar(&cmdOptions.IsPrimary, "primary", false, "Is primary index")
	fset.StringVar(&cmdOptions.With, "with", "", "index specific properties")
	// options for build-indexes, drop-indexes
	fset.StringVar(&bindexes, "indexes", "", "csv list of bucket.index to build")
	// options for Range, Statistics, Count
	fset.StringVar(&low, "low", "[]", "Span.Range: [low]")
	fset.StringVar(&high, "high", "[]", "Span.Range: [high]")
	fset.StringVar(&equal, "equal", "", "Span.Lookup: [key]")
	fset.UintVar(&inclusion, "incl", 0, "Range: 0|1|2|3")
	fset.Int64Var(&cmdOptions.Limit, "limit", 10, "Row limit")
	fset.BoolVar(&cmdOptions.Help, "h", false, "print help")
	// options for setting configuration
	fset.StringVar(&cmdOptions.ConfigKey, "ckey", "", "Config key")
	fset.StringVar(&cmdOptions.ConfigVal, "cval", "", "Config value")
	fset.StringVar(&cmdOptions.Using, "using", c.ForestDB, "storate type to use")

	// not useful to expose in sherlock
	cmdOptions.ExprType = "N1QL"
	cmdOptions.PartnStr = "partn"

	if err := fset.Parse(arguments); err != nil {
		return nil, nil, fset, err
	}

	// if server is not specified, try guessing
	if cmdOptions.Server == "•" {
		cmdOptions.Server = guessServer()
	}

	// if server is not specified, try guessing
	if cmdOptions.Auth == "•" {
		cmdOptions.Auth = guessAuth(cmdOptions.Server)
	}

	// validate combinations
	err := validate(cmdOptions, fset)
	if err != nil {
		return nil, nil, fset, err
	}
	// bindexes
	if len(bindexes) > 0 {
		cmdOptions.Bindexes = strings.Split(bindexes, ",")
	}

	// inclusion, secStrs, equal, low, high
	cmdOptions.Inclusion = qclient.Inclusion(inclusion)
	cmdOptions.SecStrs = make([]string, 0)
	if fields != "" {
		for _, field := range strings.Split(fields, ",") {
			expr, err := n1ql.ParseExpression(field)
			if err != nil {
				msgf := "Error occured: Invalid field (%v) %v\n"
				return nil, nil, fset, fmt.Errorf(msgf, field, err)
			}
			secStr := expression.NewStringer().Visit(expr)
			cmdOptions.SecStrs = append(cmdOptions.SecStrs, secStr)
		}
	}
	if equal != "" {
		cmdOptions.Equal = c.SecondaryKey(Arg2Key([]byte(equal)))
	}
	cmdOptions.Low = c.SecondaryKey(Arg2Key([]byte(low)))
	cmdOptions.High = c.SecondaryKey(Arg2Key([]byte(high)))

	// with
	if len(cmdOptions.With) > 0 {
		err := json.Unmarshal([]byte(cmdOptions.With), &cmdOptions.WithPlan)
		if err != nil {
			logging.Fatalf("%v\n", err)
			os.Exit(1)
		}
	}

	// setup cbauth
	if cmdOptions.Auth != "" {
		up := strings.Split(cmdOptions.Auth, ":")
		_, err := cbauth.InternalRetryDefaultInit(cmdOptions.Server, up[0], up[1])
		if err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s\n", err)
			os.Exit(1)
		}
	}

	return cmdOptions, fset.Args(), fset, err
}

// HandleCommand after parsing it with ParseArgs().
func HandleCommand(
	client *qclient.GsiClient,
	cmd *Command,
	verbose bool,
	w io.Writer) (err error) {

	iname, bucket, limit := cmd.IndexName, cmd.Bucket, cmd.Limit
	low, high, equal, incl := cmd.Low, cmd.High, cmd.Equal, cmd.Inclusion

	indexes, err := client.Refresh()

	entries := 0
	callb := func(res qclient.ResponseReader) bool {
		if res.Error() != nil {
			fmt.Fprintln(w, "Error: ", res)
		} else if skeys, pkeys, err := res.GetEntries(); err != nil {
			fmt.Fprintln(w, "Error: ", err)
		} else {
			if verbose == false {
				for i, pkey := range pkeys {
					fmt.Fprintf(w, "%v ... %v\n", skeys[i], string(pkey))
				}
			}
			entries += len(pkeys)
		}
		return true
	}

	switch cmd.OpType {
	case "nodes":
		fmt.Fprintln(w, "List of nodes:")
		nodes, err := client.Nodes()
		if err != nil {
			return err
		}
		for _, n := range nodes {
			fmsg := "    {%v, %v, %q}\n"
			fmt.Fprintf(w, fmsg, n.Adminport, n.Queryport, n.Status)
		}

	case "list":
		if err != nil {
			return err
		}
		fmt.Fprintln(w, "List of indexes:")
		for _, index := range indexes {
			printIndexInfo(w, index)
		}

	case "create":
		var defnID uint64
		if len(cmd.SecStrs) == 0 && !cmd.IsPrimary || cmd.IndexName == "" {
			return fmt.Errorf("createIndex(): required fields missing")
		}
		defnID, err = client.CreateIndex(
			iname, bucket, cmd.Using, cmd.ExprType,
			cmd.PartnStr, cmd.WhereStr, cmd.SecStrs, cmd.IsPrimary,
			[]byte(cmd.With))
		if err == nil {
			fmt.Fprintf(w, "Index created: %v with %q\n", defnID, cmd.With)
		}

	case "build":
		defnIDs := make([]uint64, 0, len(cmd.Bindexes))
		for _, bindex := range cmd.Bindexes {
			v := strings.Split(bindex, ":")
			if len(v) < 0 {
				return fmt.Errorf("invalid index specified : %v", bindex)
			}
			bucket, iname = v[0], v[1]
			defnID, ok := GetDefnID(client, bucket, iname)
			if ok {
				defnIDs = append(defnIDs, defnID)
			} else {
				err = fmt.Errorf("index %v/%v unknown", bucket, iname)
				break
			}
		}
		if err == nil {
			err = client.BuildIndexes(defnIDs)
			fmt.Fprintf(w, "Index building for: %v\n", defnIDs)
		}

	case "drop":
		defnID, ok := GetDefnID(client, cmd.Bucket, cmd.IndexName)
		if !ok {
			return fmt.Errorf("invalid index specified : %v", cmd.IndexName)
		}
		err = client.DropIndex(defnID)
		if err == nil {
			fmt.Fprintf(w, "Index dropped %v/%v\n", bucket, iname)
		} else {
			err = fmt.Errorf("index %v/%v drop failed", bucket, iname)
			break
		}

	case "scan":
		var state c.IndexState

		defnID, _ := GetDefnID(client, bucket, iname)
		fmt.Fprintln(w, "Scan index:")
		_, err = WaitUntilIndexState(
			client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
			100 /*period*/, 20000 /*timeout*/)

		if err != nil {
			state, err = client.IndexState(defnID)
			fmt.Fprintf(w, "Index state: {%v, %v}\n", state, err)
		} else if cmd.Equal != nil {
			equals := []c.SecondaryKey{cmd.Equal}
			client.Lookup(
				uint64(defnID), equals, false, limit,
				c.AnyConsistency, nil, callb)
		} else {
			err = client.Range(
				uint64(defnID), low, high, incl, false, limit,
				c.AnyConsistency, nil, callb)
		}
		if err == nil {
			fmt.Fprintln(w, "Total number of entries: ", entries)
		}

	case "scanAll":
		var state c.IndexState

		defnID, _ := GetDefnID(client, bucket, iname)
		fmt.Fprintln(w, "ScanAll index:")
		_, err = WaitUntilIndexState(
			client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
			100 /*period*/, 20000 /*timeout*/)
		if err != nil {
			state, err = client.IndexState(defnID)
			fmt.Fprintf(w, "Index state: {%v, %v} \n", state, err)
		} else {
			err = client.ScanAll(
				uint64(defnID), limit, c.AnyConsistency, nil, callb)
		}
		if err == nil {
			fmt.Fprintln(w, "Total number of entries: ", entries)
		}

	case "stats":
		var state c.IndexState
		var statsResp c.IndexStatistics

		defnID, _ := GetDefnID(client, bucket, iname)
		_, err = WaitUntilIndexState(
			client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
			100 /*period*/, 20000 /*timeout*/)
		if err != nil {
			state, err = client.IndexState(defnID)
			fmt.Fprintf(w, "Index state: {%v, %v} \n", state, err)
		} else if cmd.Equal != nil {
			statsResp, err = client.LookupStatistics(uint64(defnID), equal)
		} else {
			statsResp, err = client.RangeStatistics(
				uint64(defnID), low, high, incl)
		}
		if err == nil {
			fmt.Fprintln(w, "Stats: ", statsResp)
		}

	case "count":
		var state c.IndexState
		var count int64

		defnID, _ := GetDefnID(client, bucket, iname)
		_, err = WaitUntilIndexState(
			client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
			100 /*period*/, 20000 /*timeout*/)
		if err != nil {
			state, err = client.IndexState(defnID)
			fmt.Fprintf(w, "Index state: {%v, %v} \n", state, err)
		} else if cmd.Equal != nil {
			fmt.Fprintln(w, "CountLookup:")
			equals := []c.SecondaryKey{cmd.Equal}
			count, err := client.CountLookup(uint64(defnID), equals, c.AnyConsistency, nil)
			if err == nil {
				fmt.Fprintf(w, "Index %q/%q has %v entries\n", bucket, iname, count)
			}

		} else {
			fmt.Fprintln(w, "CountRange:")
			count, err = client.CountRange(uint64(defnID), low, high, incl, c.AnyConsistency, nil)
			if err == nil {
				fmt.Fprintf(w, "Index %q/%q has %v entries\n", bucket, iname, count)
			}
		}

	case "config":
		nodes, err := client.Nodes()
		if err != nil {
			return err
		}
		var adminurl string
		for _, indexer := range nodes {
			adminurl = indexer.Adminport
			break
		}
		host, sport, _ := net.SplitHostPort(adminurl)
		iport, _ := strconv.Atoi(sport)
		client := http.Client{}

		//
		// hack, fix this
		//
		ihttp := iport + 2
		url := "http://" + host + ":" + strconv.Itoa(ihttp) + "/settings"

		oreq, err := http.NewRequest("GET", url, nil)
		if cmd.Auth != "" {
			up := strings.Split(cmd.Auth, ":")
			oreq.SetBasicAuth(up[0], up[1])
		}

		oresp, err := client.Do(oreq)
		if err != nil {
			return err
		}
		obody, err := ioutil.ReadAll(oresp.Body)
		if err != nil {
			return err
		}
		oresp.Body.Close()

		pretty := strings.Replace(string(obody), ",\"", ",\n\"", -1)
		fmt.Printf("Current Settings:\n%s\n", string(pretty))

		var jbody map[string]interface{}
		err = json.Unmarshal(obody, &jbody)
		if err != nil {
			return err
		}

		if len(cmd.ConfigKey) > 0 {
			fmt.Printf("Changing config key '%s' to value '%s'\n", cmd.ConfigKey, cmd.ConfigVal)
			jbody[cmd.ConfigKey] = cmd.ConfigVal

			pbody, err := json.Marshal(jbody)
			if err != nil {
				return err
			}
			preq, err := http.NewRequest("POST", url, bytes.NewBuffer(pbody))
			if cmd.Auth != "" {
				up := strings.Split(cmd.Auth, ":")
				preq.SetBasicAuth(up[0], up[1])
			}
			_, err = client.Do(preq)
			if err != nil {
				return err
			}
			nresp, err := client.Do(oreq)
			if err != nil {
				return err
			}
			nbody, err := ioutil.ReadAll(nresp.Body)
			if err != nil {
				return err
			}
			pretty = strings.Replace(string(nbody), ",\"", ",\n\"", -1)
			fmt.Printf("New Settings:\n%s\n", string(pretty))
		}
	}
	return err
}

func printIndexInfo(w io.Writer, index *mclient.IndexMetadata) {
	defn := index.Definition
	fmt.Fprintf(w, "Index:%s/%s, Id:%v, Using:%s, Exprs:%v, isPrimary:%v\n",
		defn.Bucket, defn.Name, defn.DefnId, defn.Using, defn.SecExprs,
		defn.IsPrimary)
	insts := index.Instances
	if len(insts) < 1 {
		fmt.Fprintf(w, "    Error: zero instances")
	} else {
		fmt.Fprintf(w, "    State:%s, Error:%v\n", insts[0].State, insts[0].Error)
	}
}

// GetDefnID for bucket/indexName.
func GetDefnID(
	client *qclient.GsiClient,
	bucket, indexName string) (defnID uint64, ok bool) {

	indexes, err := client.Refresh()
	if err != nil {
		logging.Fatalf("%v\n", err)
		os.Exit(1)
	}
	for _, index := range indexes {
		defn := index.Definition
		if defn.Bucket == bucket && defn.Name == indexName {
			return uint64(index.Definition.DefnId), true
		}
	}
	return 0, false
}

// WaitUntilIndexState comes to desired `state`,
// retry for every `period` mS until `timeout` mS.
func WaitUntilIndexState(
	client *qclient.GsiClient, defnIDs []uint64,
	state c.IndexState, period, timeout time.Duration) ([]c.IndexState, error) {

	expired := time.After(timeout * time.Millisecond)
	states := make([]c.IndexState, len(defnIDs))
	pending := len(defnIDs)
	for {
		select {
		case <-expired:
			return nil, errors.New("timeout")

		default:
		}
		for i, defnID := range defnIDs {
			if states[i] != state {
				st, err := client.IndexState(defnID)
				if err != nil {
					return nil, err
				} else if st == state {
					states[i] = state
					pending--
					continue
				}
			}
		}
		if pending == 0 {
			return states, nil
		}
		time.Sleep(period * time.Millisecond)
	}
}

//----------------
// local functions
//----------------

// Arg2Key convert JSON string to golang-native.
func Arg2Key(arg []byte) []interface{} {
	var key []interface{}
	if err := json.Unmarshal(arg, &key); err != nil {
		logging.Fatalf("%v\n", err)
		os.Exit(1)
	}
	return key
}

func validate(cmd *Command, fset *flag.FlagSet) error {
	var have []string
	var dont []string

	switch cmd.OpType {
	case "":
		have = []string{}
		dont = []string{"type", "index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "ckey", "cval"}

	case "nodes":
		have = []string{"type", "server", "auth"}
		dont = []string{"h", "index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "ckey", "cval"}

	case "list":
		have = []string{"type", "server", "auth"}
		dont = []string{"h", "index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "ckey", "cval"}

	case "create":
		have = []string{"type", "server", "auth", "index", "bucket", "primary"}
		dont = []string{"h", "indexes", "low", "high", "equal", "incl", "limit", "ckey", "cval"}

	case "build":
		have = []string{"type", "server", "auth", "indexes"}
		dont = []string{"h", "index", "bucket", "where", "fields", "primary", "with", "low", "high", "equal", "incl", "limit", "ckey", "cval"}

	case "drop":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit", "ckey", "cval"}

	case "scan":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "ckey", "cval"}

	case "scanAll":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "ckey", "cval"}

	case "stats":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "limit", "ckey", "cval"}

	case "count":
		have = []string{"type", "server", "auth", "index", "bucket"}
		dont = []string{"h", "where", "fields", "primary", "with", "indexes", "ckey", "cval"}

	case "config":
		have = []string{"type", "server", "auth"}
		dont = []string{"h", "index", "bucket", "where", "fields", "primary", "with", "indexes", "low", "high", "equal", "incl", "limit"}

	default:
		return fmt.Errorf("Specified operation type '%s' has no validation rule. Please add one to use.", cmd.OpType)
	}

	err := mustHave(fset, have...)
	if err != nil {
		return err
	}

	err = mustNotHave(fset, dont...)
	if err != nil {
		return err
	}

	return nil
}

func mustHave(fset *flag.FlagSet, keys ...string) error {
	for _, key := range keys {
		found := false
		fset.Visit(
			func(f *flag.Flag) {
				if f.Name == key {
					found = true
				}
			})
		if !found {
			flag := fset.Lookup(key)
			if flag == nil || flag.DefValue == "" {
				return fmt.Errorf("Invalid flags. Flag '%s' is required for this operation", key)
			}
		}
	}
	return nil
}

func mustNotHave(fset *flag.FlagSet, keys ...string) error {
	for _, key := range keys {
		found := false
		fset.Visit(
			func(f *flag.Flag) {
				if f.Name == key {
					found = true
				}
			})
		if found {
			return fmt.Errorf("Invalid flags. Flag '%s' cannot appear for this operation", key)
		}
	}
	return nil
}

func guessServer() string {
	ports := []string{"8091", "9000"}
	for _, port := range ports {
		server := "localhost:" + port
		resp, err := http.Get("http://" + server + "/pools")
		if err != nil {
			continue
		}
		resp.Body.Close()
		return server
	}
	return ""
}

func guessAuth(server string) string {
	auths := []string{"Administrator:asdasd", "Administrator:couchbase"}
	client := http.Client{}
	for _, auth := range auths {
		up := strings.Split(auth, ":")
		req, err := http.NewRequest("GET", "http://"+server+"/settings/web", nil)
		req.SetBasicAuth(up[0], up[1])
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			continue
		}
		resp.Body.Close()
		return auth
	}
	return ""
}
