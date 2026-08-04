Feature: Open a session against R-Event
  As an SDK consumer
  I want to open a session to a live R-Event server
  So that my client gets registered

  Scenario: Client connects and gets registered
    Given the server is running
    When I open the SDK session
    Then the client should be registered by the server
    When I cancel the SDK session context
    Then the session should finish with context canceled

  Scenario: Session fails when server is unavailable
    When I open the SDK session
    Then the session should fail with CantConnectToServerError

  Scenario: Client connects, gets registered, server restarts and client reconnects
    Given the server is running
    When I open the SDK session
    Then the client should be registered by the server
    When the server restarts
    Then the client should be registered by the server
