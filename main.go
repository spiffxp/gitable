package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/oauth2"

	airtable "github.com/fabioberger/airtable-go"
	"github.com/genuinetools/pkg/cli"
	"github.com/google/go-github/v22/github"
	"github.com/jessfraz/gitable/version"
	"github.com/sirupsen/logrus"
)

const (
	githubPageSize = 100
)

type options struct {
	interval time.Duration
	autofill bool
	once     bool

	githubToken string
	orgs        stringSlice
	watched     bool
	watchSince  string

	airtableAPIKey    string
	airtableBaseID    string
	airtableTableName string

	debug bool
}

func (o *options) validate() error {
	if len(o.githubToken) < 1 {
		return errors.New("gitHub token cannot be empty")
	}
	if len(o.airtableAPIKey) < 1 {
		return errors.New("airtable API Key cannot be empty")
	}
	if len(o.airtableBaseID) < 1 {
		return errors.New("airtable Base ID cannot be empty")
	}
	if len(o.airtableTableName) < 1 {
		return errors.New("airtable Table cannot be empty")
	}
	return nil
}

// stringSlice is a slice of strings
type stringSlice []string

// implement the flag interface for stringSlice
func (s *stringSlice) String() string {
	return fmt.Sprintf("%s", *s)
}
func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	o := options{}
	// Create a new cli program.
	p := cli.NewProgram()
	p.Name = "gitable"
	p.Description = "Bot to automatically sync and update an airtable sheet with GitHub pull request and issue data"
	// Set the GitCommit and Version.
	p.GitCommit = version.GITCOMMIT
	p.Version = version.VERSION

	// Setup the global flags.
	p.FlagSet = flag.NewFlagSet("global", flag.ExitOnError)
	p.FlagSet.DurationVar(&o.interval, "interval", time.Minute, "update interval (ex. 5ms, 10s, 1m, 3h)")
	p.FlagSet.BoolVar(&o.autofill, "autofill", false, "autofill all pull requests and issues for a user [or orgs] to a table (defaults to current user unless --orgs is set)")
	p.FlagSet.BoolVar(&o.once, "once", false, "run once and exit, do not run as a daemon")

	p.FlagSet.StringVar(&o.githubToken, "github-token", os.Getenv("GITHUB_TOKEN"), "GitHub API token (or env var GITHUB_TOKEN)")
	p.FlagSet.Var(&o.orgs, "orgs", "organizations to include (this option only applies to --autofill)")

	p.FlagSet.StringVar(&o.airtableAPIKey, "airtable-apikey", os.Getenv("AIRTABLE_APIKEY"), "Airtable API Key (or env var AIRTABLE_APIKEY)")
	p.FlagSet.StringVar(&o.airtableBaseID, "airtable-baseid", os.Getenv("AIRTABLE_BASEID"), "Airtable Base ID (or env var AIRTABLE_BASEID)")
	p.FlagSet.StringVar(&o.airtableTableName, "airtable-table", os.Getenv("AIRTABLE_TABLE"), "Airtable Table (or env var AIRTABLE_TABLE)")

	p.FlagSet.BoolVar(&o.watched, "watched", false, "include the watched repositories")
	p.FlagSet.StringVar(&o.watchSince, "watch-since", "2008-01-01T00:00:00Z", "defines the starting point of the issues been watched (format: 2006-01-02T15:04:05Z). defaults to no filter")

	p.FlagSet.BoolVar(&o.debug, "debug", false, "enable debug logging")
	p.FlagSet.BoolVar(&o.debug, "d", false, "enable debug logging")

	// Set the before function.
	p.Before = func(ctx context.Context) error {
		// Set the log level.
		if o.debug {
			logrus.SetLevel(logrus.DebugLevel)
		}
		return o.validate()
	}

	// Set the main program action.
	p.Action = func(ctx context.Context, args []string) error {
		ticker := time.NewTicker(o.interval)

		// On ^C, or SIGTERM handle exit.
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		signal.Notify(c, syscall.SIGTERM)
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		go func() {
			for sig := range c {
				logrus.Infof("Received %s, exiting.", sig.String())
				ticker.Stop()
				cancel()
				os.Exit(0)
			}
		}()

		// Create the http client.
		ts := oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: o.githubToken},
		)
		tc := oauth2.NewClient(ctx, ts)

		// Create the github client.
		ghClient := github.NewClient(tc)

		ghUser, _, err := ghClient.Users.Get(ctx, "")
		if err != nil {
			logrus.Fatalf("getting current github user for token failed: %v", err)
		}

		// Create the airtable client.
		airtableClient, err := airtable.New(o.airtableAPIKey, o.airtableBaseID)
		if err != nil {
			logrus.Fatal(err)
		}

		// Affiliation must be set before we add the user to the "orgs".
		affiliation := "owner,collaborator"
		if len(o.orgs) > 0 {
			affiliation += ",organization_member"
		}

		// If we didn't get any orgs explicitly passed, use the current user.
		if len(o.orgs) == 0 {
			// Add the current github user to orgs.
			o.orgs = append(o.orgs, ghUser.GetLogin())
		}

		// Create our bot type.
		bot := &bot{
			ghLogin:        ghUser.GetLogin(),
			ghClient:       ghClient,
			airtableClient: airtableClient,
			// Initialize our map.
			issues: map[string]*github.Issue{},
		}

		// If the user passed the once flag, just do the run once and exit.
		if o.once {
			if err := bot.run(ctx, affiliation, o.airtableTableName, o.autofill, o.orgs, o.watched, o.watchSince); err != nil {
				logrus.Fatal(err)
			}
			logrus.Infof("Updated airtable table %s for base %s", o.airtableTableName, o.airtableBaseID)
			os.Exit(0)
		}

		logrus.Infof("Starting bot to update airtable table %s for base %s every %s", o.airtableTableName, o.airtableBaseID, o.interval)
		for range ticker.C {
			if err := bot.run(ctx, affiliation, o.airtableTableName, o.autofill, o.orgs, o.watched, o.watchSince); err != nil {
				logrus.Fatal(err)
			}
		}
		return nil
	}

	// Run our program.
	p.Run()
}

type bot struct {
	ghLogin        string
	issues         map[string]*github.Issue
	ghClient       *github.Client
	airtableClient *airtable.Client
}

// githubRecord holds the data for the airtable fields that define the github data.
type githubRecord struct {
	ID     string `json:"id,omitempty"`
	Fields Fields `json:"fields,omitempty"`
}

// Fields defines the airtable fields for the data.
type Fields struct {
	Reference  string
	Title      string
	State      string
	Author     string
	Type       string
	Labels     []string
	Comments   int
	URL        string
	Updated    time.Time
	Created    time.Time
	Completed  time.Time
	Project    interface{}
	Repository string
}

func (bot *bot) run(ctx context.Context, affiliation string, airtableTableName string, autofill bool, orgs stringSlice, watched bool, watchSince string) error {
	// if we are in autofill mode, get our repositories
	if autofill {
		if err := bot.getRepositories(ctx, affiliation, orgs); err != nil {
			logrus.Errorf("Failed to get repos, %v\n", err)
			return err
		}
	}

	ghRecords := []githubRecord{}
	if err := bot.airtableClient.ListRecords(airtableTableName, &ghRecords); err != nil {
		return fmt.Errorf("listing records for table %s failed: %v", airtableTableName, err)
	}

	since, err := time.Parse("2006-01-02T15:04:05Z", watchSince)
	if err != nil {
		return err
	}

	for _, record := range ghRecords {
		if record.Fields.Updated.After(since) {
			since = record.Fields.Updated
		}
	}

	// if we are in watching mode, get your watched repositories
	if watched {
		if err := bot.getWatchedRepositories(ctx, since); err != nil {
			logrus.Errorf("Failed to get watched repos, %v\n", err)
			return err
		}
	}

	// Iterate over the records.
	for _, record := range ghRecords {
		// Parse the reference.
		user, repo, id, err := parseReference(record.Fields.Reference)
		if err != nil {
			logrus.Infof("Reference for %v failed:\n%v\n", record, err)
			continue
		}

		// Get the github issue.
		var issue *github.Issue

		// Check if we already have it from autofill or watched.
		if autofill || watched {
			if i, ok := bot.issues[record.Fields.Reference]; ok {
				logrus.Debugf("found github issue %s from autofill", record.Fields.Reference)
				issue = i
				// delete the key from the autofilled map
				delete(bot.issues, record.Fields.Reference)
			}
		}

		// If we don't already have the issue, then get it.
		if issue == nil {
			logrus.Debugf("getting issue %s", record.Fields.Reference)
			issue, _, err = bot.ghClient.Issues.Get(ctx, user, repo, id)
			if err != nil {
				if strings.Contains(err.Error(), "404 Not Found") {
					// Delete it from the table, the repo has probably moved or something.
					if err := bot.airtableClient.DestroyRecord(airtableTableName, record.ID); err != nil {
						logrus.Warnf("destroying record %s failed: %v", record.ID, err)
					}
					continue
				}
				return fmt.Errorf("getting issue %s failed: %v", record.Fields.Reference, err)
			}
		}

		if err := bot.applyRecordToTable(ctx, issue, record.Fields.Reference, record.ID, airtableTableName); err != nil {
			return err
		}
	}

	// If we autofilled issues, loop over and create which ever ones remain.
	for key, issue := range bot.issues {
		if err := bot.applyRecordToTable(ctx, issue, key, "", airtableTableName); err != nil {
			logrus.Errorf("Failed to apply record to table for reference %s because %v\n", key, err)
			continue
		}
	}

	return nil
}

func (bot *bot) applyRecordToTable(ctx context.Context, issue *github.Issue, key, id string, airtableTableName string) error {
	// Trim surrounding quotes from ID string.
	id = strings.Trim(id, "\"")

	// Parse the reference.
	user, repo, number, err := parseReference(key)
	if err != nil {
		return err
	}

	// Iterate over the labels.
	labels := []string{}
	for _, label := range issue.Labels {
		labels = append(labels, label.GetName())
	}

	issueType := "issue"
	if issue.IsPullRequest() {
		issueType = "pull request"
		// If the status is closed, we should find out if the
		// _actual_ pull request status is "merged".
		merged, _, err := bot.ghClient.PullRequests.IsMerged(ctx, user, repo, number)
		if err != nil {
			return err
		}
		if merged {
			mstr := "merged"
			issue.State = &mstr
		}
	}

	// Create our empty record struct.
	record := githubRecord{
		ID: id,
		Fields: Fields{
			Reference:  key,
			Title:      issue.GetTitle(),
			State:      issue.GetState(),
			Author:     issue.GetUser().GetLogin(),
			Type:       issueType,
			Comments:   issue.GetComments(),
			URL:        issue.GetHTMLURL(),
			Updated:    issue.GetUpdatedAt(),
			Created:    issue.GetCreatedAt(),
			Completed:  issue.GetClosedAt(),
			Repository: repo,
		},
	}

	// Update the record fields.
	fields := map[string]interface{}{
		"Reference":  record.Fields.Reference,
		"Title":      record.Fields.Title,
		"State":      record.Fields.State,
		"Author":     record.Fields.Author,
		"Type":       record.Fields.Type,
		"Comments":   record.Fields.Comments,
		"URL":        record.Fields.URL,
		"Updated":    record.Fields.Updated,
		"Created":    record.Fields.Created,
		"Completed":  record.Fields.Completed,
		"Repository": record.Fields.Repository,
	}

	if id != "" {
		// If we were passed a record ID, update the record instead of create.
		logrus.Debugf("updating record %s for issue %s", id, key)
		if err := bot.airtableClient.UpdateRecord(airtableTableName, id, fields, &record); err != nil {
			logrus.Warnf("updating record %s for issue %s failed: %v", id, key, err)
			return nil
		}
	} else {
		// Create the field.
		logrus.Debugf("creating new record for issue %s", key)
		if err := bot.airtableClient.CreateRecord(airtableTableName, &record); err != nil {
			return err
		}
	}

	// Try again with labels, since the user may not have pre-populated the label options.
	// TODO: add a create multiple select when the airtable API supports it.
	fields["Labels"] = labels
	if err := bot.airtableClient.UpdateRecord(airtableTableName, record.ID, fields, &record); err != nil {
		logrus.Warnf("updating record with labels %s for issue %s failed: %v", record.ID, key, err)
	}

	return nil
}

func (bot *bot) getRepositories(ctx context.Context, affiliation string, orgs stringSlice) error {
	logrus.Infof("getting repositories for org[s]: %s...", strings.Join(orgs, ", "))
	opt := &github.RepositoryListOptions{
		Affiliation: affiliation,
		ListOptions: github.ListOptions{
			Page:    1,
			PerPage: githubPageSize,
		},
	}
	var allRepos []*github.Repository
	for {
		repos, resp, err := bot.ghClient.Repositories.List(ctx, "", opt)
		if err != nil {
			return err
		}
		allRepos = append(allRepos, repos...)
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	for _, repo := range allRepos {
		logrus.Debugf("checking if %s is in (%s)", repo.GetOwner().GetLogin(), strings.Join(orgs, " | "))
		if in(orgs, repo.GetOwner().GetLogin()) {
			logrus.Debugf("getting issues for repo %s...", repo.GetFullName())
			if err := bot.getIssues(ctx, repo.GetOwner().GetLogin(), repo.GetName(), repo.UpdatedAt.Time); err != nil {
				logrus.Debugf("Failed to get issues for repo %s - %v\n", repo.GetName(), err)
				return err
			}
		}
	}

	return nil
}

func (bot *bot) getWatchedRepositories(ctx context.Context, since time.Time) error {
	logrus.Infof("getting repositories watched by %s...", bot.ghLogin)
	opt := &github.ListOptions{
		Page:    1,
		PerPage: githubPageSize,
	}
	var allRepos []*github.Repository
	for {
		repos, resp, err := bot.ghClient.Activity.ListWatched(ctx, "", opt)
		if err != nil {
			return err
		}
		allRepos = append(allRepos, repos...)
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	for _, repo := range allRepos {
		logrus.Infof("getting issues for repo %s...", repo.GetFullName())
		if err := bot.getIssues(ctx, repo.GetOwner().GetLogin(), repo.GetName(), since); err != nil {
			return err
		}
	}
	return nil
}

func (bot *bot) getIssues(ctx context.Context, owner, repo string, since time.Time) error {
	opt := &github.IssueListByRepoOptions{
		State: "all",
		Since: since,
		ListOptions: github.ListOptions{
			Page:    1,
			PerPage: githubPageSize,
		},
	}
	var allIssues []*github.Issue
	for {
		issues, resp, err := bot.ghClient.Issues.ListByRepo(ctx, owner, repo, opt)
		if err != nil {
			return err
		}
		allIssues = append(allIssues, issues...)
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	for _, issue := range allIssues {
		key := createReference(owner, repo, issue.GetNumber())
		// logrus.Debugf("handling issue %s...", key)
		bot.issues[key] = issue
	}
	return nil
}

// convert ("org", "repo", 123) into "org/repo#123"
func createReference(owner string, repo string, number int) string {
	return fmt.Sprintf("%s/%s#%d", owner, repo, number)
}

// split "org/repo#123" into ("org", "repo", 123)
func parseReference(ref string) (string, string, int, error) {
	// Split the reference into repository and issue number.
	parts := strings.SplitN(ref, "#", 2)
	if len(parts) < 2 {
		return "", "", 0, fmt.Errorf("could not parse reference name into repository and issue number for %s, got: %#v", ref, parts)
	}
	repolong := parts[0]
	i := parts[1]

	// Parse the string id into an int.
	id, err := strconv.Atoi(i)
	if err != nil {
		return "", "", 0, err
	}

	// Split the repo name into owner and repo.
	parts = strings.SplitN(repolong, "/", 2)
	if len(parts) < 2 {
		return "", "", 0, fmt.Errorf("could not parse reference name into owner and repo for %s, got: %#v", repolong, parts)
	}

	return parts[0], parts[1], id, nil
}

func in(a stringSlice, s string) bool {
	for _, b := range a {
		if b == s {
			return true
		}
	}
	return false
}
