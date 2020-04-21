package killrvideo

import (
	"fmt"
	"github.com/gocql/gocql"
	. "github.com/onsi/gomega"
)

func InsertUserMutation(id interface{}, firstname interface{}, email interface{}, ifNotExists bool) string {
	query := `mutation {
	  insertUsers(value:{userid:%s, firstname:%s, email:%s}%s) {
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

	conditionalParameter := ""
	if ifNotExists {
		conditionalParameter = ", ifNotExists: true"
	}

	return fmt.Sprintf(
		query, asGraphQLString(id), asGraphQLString(firstname), asGraphQLString(email), conditionalParameter)
}

func UpdateUserMutation(id string, firstname string, email string, ifEmail string) string {
	query := `mutation {
	  updateUsers(value:{userid:%s, firstname:%s, email:%s}%s) {
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

func DeleteUserMutation(id string, ifNotName string) string {
	query := `mutation {
	  deleteUsers(value:{userid:%s}%s) {
		applied
		value {
		  userid
		  firstname
          email
		}
	  }
	}`

	conditionalParameter := ""
	if ifNotName != "" {
		conditionalParameter = fmt.Sprintf(`, ifCondition: { firstname: {notEq: "%s"}}`, ifNotName)
	}

	return fmt.Sprintf(query, asGraphQLString(id), conditionalParameter)
}

func SelectUserQuery(id string) string {
	query := `query {
	  users(value:{userid:%s}) {
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

func SelectTagsByLetter(firstLetter string, pageSize int, pageState string) string {
	query := `{
		tagsByLetter(value: {firstLetter: %s}, options: {pageSize: %d, pageState: %s}){
  		  pageState
		  values{ tag }}
	}`

	return fmt.Sprintf(query, asGraphQLString(firstLetter), pageSize, asGraphQLString(pageState))
}

func SelectTagsByLetterNoWhereClause(pageState string) string {
	query := `{
		tagsByLetter%s {
  		  pageState
		  values{ tag }}
	}`

	params := ""
	if pageState != "" {
		params = fmt.Sprintf(`(options: {pageState: "%s"})`, pageState)
	}

	return fmt.Sprintf(query, params)
}

func CqlInsertTagByLetter(session *gocql.Session, tag string) {
	query := "INSERT INTO killrvideo.tags_by_letter (first_letter, tag) VALUES (?, ?)"
	err := session.Query(query, tag[0:1], tag).Exec()
	Expect(err).ToNot(HaveOccurred())
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

func asGraphQLString(value interface{}) string {
	if value == "" || value == nil {
		return "null"
	}
	return fmt.Sprintf(`"%s"`, value)
}
