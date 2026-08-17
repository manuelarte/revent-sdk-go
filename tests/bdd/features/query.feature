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
      | Query     | org.github.r-event.sdk.test.query  |
    Then the query "2a708250-92dd-42b3-a25f-a82c34de8c38" should fail with QueryHandlerNotFound
    When I cancel the SDK session context
    Then the session should finish with context canceled

  Scenario: Query successfully handled and responded
    Given the server is running
    And I register a handler for query "org.github.r-event.sdk.test.query"
    When I open the SDK session
    Then the client should be registered by the server
    When I send a query request
      | RequestId | 2a708250-92dd-42b3-a25f-a82c34de8c38 |
      | Query     | org.github.r-event.sdk.test.query  |
    Then the query "2a708250-92dd-42b3-a25f-a82c34de8c38" should succeed with result "handled-any"
    When I cancel the SDK session context
    Then the session should finish with context canceled
