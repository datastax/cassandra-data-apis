package killrvideo

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/data-endpoints/internal/testutil"
)

func InsertUserMutation(id string, firstname string, email string) string {
	query := `mutation {
	  insertUsers(data:{userid:%s, firstname:%s, email:%s}) {
		applied
		value {
		  userid
		  firstname
		  lastname
          email
		  createdDate
		}
	  }
	}`

	return fmt.Sprintf(query, asGraphQLString(id), asGraphQLString(firstname), asGraphQLString(email))
}

func UpdateUserMutation(id string, firstname string, email string, ifEmail string) string {
	query := `mutation {
	  updateUsers(data:{userid:%s, firstname:%s, email:%s}%s) {
		applied
		value {
		  userid
		  firstname
          email
		}
	  }
	}`

	conditionalParameter := ""
	if ifEmail != "" {
		conditionalParameter = fmt.Sprintf(`, ifCondition: { email: {eq: "%s"}}`, ifEmail)
	}

	return fmt.Sprintf(
		query, asGraphQLString(id), asGraphQLString(firstname), asGraphQLString(email), conditionalParameter)
}

func SelectUserQuery(id string) string {
	query := `query {
	  users(data:{userid:%s}) {
		pageState
		values {
		  userid
		  firstname
		  lastname
          email
		  createdDate
		}
	  }
	}`

	return fmt.Sprintf(query, asGraphQLString(id))
}

func SelectCommentsByVideoGreaterThan(videoId string, startCommentId string) string {
	query := `query {
	  commentsByVideoFilter(filter:{videoid:{eq: %s}, commentid: {gt: %s}}) {
		pageState
		values {
		  videoid
		  commentid
		  comment
          userid
		}
	  }
	}`

	return fmt.Sprintf(query, asGraphQLString(videoId), asGraphQLString(startCommentId))
}

func NewUuid() string {
	uuid, err := gocql.RandomUUID()
	testutil.PanicIfError(err)
	return uuid.String()
}

func asGraphQLString(value string) string {
	if value == "" {
		return "null"
	}
	return fmt.Sprintf(`"%s"`, value)
}
