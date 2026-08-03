Feature: Open a session against R-Event
  As an SDK consumer
  I want to open a session to a live R-Event server
  So that my client gets registered

  Scenario: Client connects and gets registered
    Given the R-Event gRPC server is running in testcontainers
    And a configured SDK state
    When I open the SDK session
    Then the client should be registered by the server
    When I cancel the SDK session context
    Then OpenSession should finish with context canceled

  Scenario: Session fails when server is unavailable
    Given a configured SDK state with an unavailable gRPC endpoint
    When I open the SDK session and wait for completion
    Then OpenSession should fail with CantConnectToServerError

