package killrvideo

import (
	"fmt"
)

func InsertUserMutation(id interface{}, firstname interface{}, email interface{}, ifNotExists bool) string {
	query := `mutation {
	  insertUsers(data:{userid:%s, firstname:%s, email:%s}%s) {
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

func DeleteUserMutation(id string, ifNotName string) string {
	query := `mutation {
	  deleteUsers(data:{userid:%s}%s) {
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

func SelectTagsByLetter(firstLetter string, pageSize int, pageState string) string {
	query := `{
		tagsByLetter(data: {firstLetter: %s}, options: {pageSize: %d, pageState: %s}){
  		  pageState
		  values{ tag }}
	}`

	return fmt.Sprintf(query, asGraphQLString(firstLetter), pageSize, asGraphQLString(pageState))
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
