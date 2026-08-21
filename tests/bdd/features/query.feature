Feature: Query features in R-Event
  As an SDK consumer
  I want to open a session to a live R-Event server
  So I can register the queries I can handle and also query.

  Scenario: Query handler not found
    Given the server is running
    When I open the SDK session
    Then the client should be registered by the server
    When I send a query request
      | RequestId | 2a708250-92dd-42b3-a25f-a82c34de8c38 |
      | Query     | org.github.r-event.sdk.test.one-param-query  |
    Then the query "2a708250-92dd-42b3-a25f-a82c34de8c38" should fail with QueryHandlerNotFound
    When I cancel the SDK session context
    Then the session should finish with context canceled

  Scenario: Query successfully handled and responded
    Given the server is running
    And I register a handler for query "org.github.r-event.sdk.test.one-param-query"
    When I open the SDK session
    Then the client should be registered by the server
    When I send a query request
      | RequestId | 2a708250-92dd-42b3-a25f-a82c34de8c38 |
      | Query     | org.github.r-event.sdk.test.one-param-query  |
    Then the query "2a708250-92dd-42b3-a25f-a82c34de8c38" should succeed with result "handled-any"
    When I cancel the SDK session context
    Then the session should finish with context canceled

  Scenario: Query with unmarshal error in response
    Given the server is running
    And I register a handler for query "org.github.r-event.sdk.test.invalid-response-query"
    When I open the SDK session
    Then the client should be registered by the server
    When I send a query request
      | RequestId | 3b808350-93ed-53c4-b36f-b93d45ef9d39 |
      | Query     | org.github.r-event.sdk.test.invalid-response-query  |
    Then the query "3b808350-93ed-53c4-b36f-b93d45ef9d39" should fail with UnmarshalError
    When I cancel the SDK session context
    Then the session should finish with context canceled

  Scenario: Handler returns error when processing query
    Given the server is running
    And I register an error-returning handler for query "org.github.r-event.sdk.test.error-query"
    When I open the SDK session
    Then the client should be registered by the server
    When I send a query request
      | RequestId | 5d910562-b5af-75e6-d58f-d15f67af1f51 |
      | Query     | org.github.r-event.sdk.test.error-query  |
    Then the query "5d910562-b5af-75e6-d58f-d15f67af1f51" should fail with QueryHandlingFailed
    When I cancel the SDK session context
    Then the session should finish with context canceled


