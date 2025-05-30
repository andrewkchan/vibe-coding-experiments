# Continuing test-fixing session discussion
_Exported on 5/30/2025 at 01:49:36 PDT from Cursor (0.50.5)_

---

**User**

Let's pick up a test-fixing session from yesterday. The logs are in the cursor_debugging_crawler_program_issues_gem25pro.md file that should be attached (we are fixing the issue where the crawler cannot scale beyond 20 workers, we implemented connection pooling, and started on adapting the tests). The last thing you were doing was justifying why you were making a formerly fairly simple test `tests/test_politeness.py::test_is_url_allowed_by_robots` fairly complicated. Do you still feel the same way today?

---

**Cursor**

You're right to bring this up. Looking back at the proposed changes for `test_is_url_allowed_by_robots` in `crawler/tests/test_politeness.py`, the mocking sequence indeed became quite involved.

My thinking yesterday was to meticulously follow the chain of calls and ensure every database interaction through the new pool mechanism was accounted for within that single test function. This led to the `side_effect` list for `get_connection` to simulate:
1.  DB check for manual exclusion.
2.  DB check for cached robots.txt (within `_get_robots_for_domain`).
3.  DB update for robots.txt cache (within `_get_robots_for_domain` after a fetch).

**Do I still feel the same way today? Not entirely. There's a better, cleaner approach for *this specific test* (`test_is_url_allowed_by_robots`), while the detailed mocking is still relevant for other tests.**

The core purpose of `test_is_url_allowed_by_robots` is to verify the logic of `is_url_allowed` itself:
*   Does it correctly check manual exclusions?
*   Does it correctly call `_get_robots_for_domain`?
*   Does it correctly use the result from `_get_robots_for_domain` (the `RobotExclusionRulesParser` object) to determine if a URL path is allowed or disallowed?

The *internal* database caching and fetching mechanics of `_get_robots_for_domain` are complex and are (or should be) tested by their own dedicated unit tests (e.g., `test_get_robots_from_db_cache_fresh`, `test_get_robots_db_cache_stale_then_fetch_success`, etc.).

**A More Standard and Simpler Approach for `test_is_url_allowed_by_robots`:**

Instead of mocking all the nested DB calls that `_get_robots_for_domain` makes, we can mock `_get_robots_for_domain` itself for the purpose of testing `is_url_allowed`.

1.  **Mock `_get_robots_for_domain`**: Within `test_is_url_allowed_by_robots`, we can patch `politeness_enforcer._get_robots_for_domain` to be an `AsyncMock`.
2.  **Configure its Return Value**: This mock can be configured to return a specific `RobotExclusionRulesParser` instance that's already set up with the desired rules (e.g., "Disallow: /disallowed").
3.  **Mock the Manual Exclusion Check**: The direct DB call made by `is_url_allowed` (for manual exclusion) still needs to be mocked via `politeness_enforcer.storage.db_pool.get_connection()`. This is simpler as it's just one DB interaction.

**This approach offers:**
*   **Focus:** `test_is_url_allowed_by_robots` focuses purely on the logic of `is_url_allowed` and its direct DB interaction, treating `_get_robots_for_domain` as a dependency that provides a (mocked) RERP object.
*   **Simplicity:** The mocking setup for this specific test becomes much less convoluted.
*   **Robustness:** Relies on the dedicated unit tests for `_get_robots_for_domain` to ensure *its* internal DB interactions (cache checks, updates through the pool) are correct. This is a standard separation of concerns in testing.

**So, my revised opinion is:**
*   The detailed pool mocking (using `get_connection.side_effect` for multiple calls) *is* necessary for tests that are specifically targeting methods like `_get_robots_for_domain` to verify their fetching and DB caching logic.
*   For `test_is_url_allowed_by_robots`, we can and should simplify by mocking out `_get_robots_for_domain` more directly.

Let's adjust `test_is_url_allowed_by_robots` with this simpler strategy. The other tests that focus on the internals of `_get_robots_for_domain` (like `test_get_robots_from_memory_cache`, `test_get_robots_from_db_cache_fresh`, etc.) will still need to mock the pool interactions carefully, as they are indeed testing how that method uses the pool.

Before we dive into fixing that, I also see the linter error:
`crawler/tests/test_politeness.py:95: error: Item "None" of "Connection | None" has no attribute "cursor" [union-attr]`
This is in `test_load_manual_exclusions_with_file`:
```python
# ...
        conn_verify = db_pool_to_check.get_connection() # conn_verify could be None if pool is empty and get_connection doesn't block or raises
        cursor = conn_verify.cursor() # This line would fail if conn_verify is None
# ...
```
The `SQLiteConnectionPool.get_connection` method, as written, raises `queue.Empty` if it cannot return a connection immediately (when `block=False` or timeout occurs without `block=True`). If `block=True` (default) and there's a timeout, it also raises `Empty`. The fixture `db_pool` initializes with `pool_size=1` and pre-fills it, so `get_connection()` in a test context using this fixture *should* typically succeed unless the connection is already held and not returned.

For test robustness, especially when `get_connection` might genuinely fail or if we want to test such scenarios, we should handle the possibility of `conn_verify` being `None` or the `Empty` exception. However, in many test contexts where the fixture guarantees a connection, we proceed assuming success. The linter is correct to point out the theoretical issue. In this specific test (`test_load_manual_exclusions_with_file`), we *expect* the `db_pool` (provided by the fixture) to give us a connection for verification. The test's purpose isn't to check pool exhaustion.

We can either:
1.  Assert `conn_verify` is not `None` before using it (good practice).
2.  Trust the `db_pool` fixture in `conftest.py` (which uses `block=True` by default in `get_connection`'s underlying queue.get and a positive timeout) to provide a connection or the test itself would fail earlier if the pool was misconfigured or empty. The current `SQLiteConnectionPool.get_connection` actually re-raises `Empty` from the queue, so it wouldn't return `None`.

The linter error is likely because `conn_from_pool: sqlite3.Connection | None = None` is a pattern used elsewhere, and it's inferring `get_connection()` *could* return `None`, even though our current implementation raises an exception instead.

Let's refine the `get_connection` in `SQLiteConnectionPool` to be more explicit about its return type if it never intends to return `None` (it should always return a `Connection` or raise).
The current `get_connection` signature is `-> sqlite3.Connection`. If it raises `Empty`, it doesn't return `None`. So the linter might be being overly cautious or picking up on the `| None` pattern from variable initializations.

For now, let's focus on simplifying `test_is_url_allowed_by_robots`.

Would you like me to proceed with refactoring `test_is_url_allowed_by_robots` using the simpler approach of mocking `_get_robots_for_domain` directly? Then we can look at the other tests.

```python
# ...
        conn_verify = db_pool_to_check.get_connection() # conn_verify could be None if pool is empty and get_connection doesn't block or raises
        cursor = conn_verify.cursor() # This line would fail if conn_verify is None
# ...
```

---

**User**

Yes let's not get distracted. Focus on fixing test_is_url_allowed_by_robots. The failure is as follows:


---

**Cursor**

Okay, let's simplify `test_is_url_allowed_by_robots` by directly mocking `_get_robots_for_domain` and only mocking the direct database interaction for the manual exclusion check. This will make the test more focused on the logic of `is_url_allowed` itself.

Here's the refined version:

```python:crawler/tests/test_politeness.py
// ... existing code ...
@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    # --- Setup Mocks ---

    # 1. Mock for the manual exclusion DB check (called directly by is_url_allowed)
    # This mock will be used for both calls to is_url_allowed in this test.
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) # Simulate domain is NOT manually excluded
    
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    
    # Configure the pool to return this connection when get_connection is called
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion

    # 2. Mock _get_robots_for_domain to return a pre-configured RERP instance
    # This prevents us from needing to mock the fetcher or _get_robots_for_domain's internal DB logic.
    rerp_instance = RobotExclusionRulesParser()
    rerp_instance.parse(robots_text)
    
    # Patch the method on the politeness_enforcer instance directly
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp_instance)

    # --- Test Allowed URL ---
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    
    # Verify interactions for the allowed_url call:
    # - Manual exclusion check should have happened
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    mock_conn_manual_exclusion.cursor.assert_called_once()
    mock_cursor_manual_exclusion.execute.assert_called_once() # Check query if needed, but existence is key
    mock_cursor_manual_exclusion.fetchone.assert_called_once()
    # - _get_robots_for_domain should have been called
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # --- Reset mocks for the next call ---
    politeness_enforcer.storage.db_pool.get_connection.reset_mock()
    mock_conn_manual_exclusion.cursor.reset_mock()
    mock_cursor_manual_exclusion.execute.reset_mock()
    mock_cursor_manual_exclusion.fetchone.reset_mock()
    politeness_enforcer._get_robots_for_domain.reset_mock()
    # Re-apply the same return value for manual exclusion check, as it might be called again
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) 
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion
    # The _get_robots_for_domain mock is still in place with its return_value

    # --- Test Disallowed URL ---
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False

    # Verify interactions for the disallowed_url call:
    # - Manual exclusion check
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    mock_conn_manual_exclusion.cursor.assert_called_once()
    # - _get_robots_for_domain should have been called again
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)
// ... existing code ...
```

**Explanation of Changes:**

1.  **Removed Complex `side_effect`**: We no longer try to mock the entire sequence of `_get_robots_for_domain`'s internal DB calls.
2.  **Mocking Manual Exclusion Check**:
    *   We set up `politeness_enforcer.storage.db_pool.get_connection` to return a `mock_conn_manual_exclusion`.
    *   This `mock_conn_manual_exclusion`'s cursor is configured to return `(0,)` for `fetchone()`, indicating the domain is not manually excluded.
3.  **Mocking `_get_robots_for_domain`**:
    *   `politeness_enforcer._get_robots_for_domain` is replaced with an `AsyncMock`.
    *   This `AsyncMock` is configured to `return_value` a `RobotExclusionRulesParser` instance that has been pre-loaded with the `robots_text`.
4.  **Verification**:
    *   For each call to `is_url_allowed`, we verify that the manual exclusion check (the call to `get_connection`) happened.
    *   We also verify that our mocked `_get_robots_for_domain` was called with the correct domain.
5.  **Resetting Mocks**: Between testing `allowed_url` and `disallowed_url`, relevant mocks are reset to ensure assertions are clean for each case. The `_get_robots_for_domain` mock remains in place with its behavior.

This approach makes `test_is_url_allowed_by_robots` significantly simpler and more focused. It tests the direct responsibilities of `is_url_allowed`: checking manual exclusions and then using the outcome of `_get_robots_for_domain`. The correctness of `_get_robots_for_domain` itself (including its DB caching and fetching logic) is delegated to other, more specific tests.

This should resolve the `AttributeError: Mock object has no attribute 'conn'` and provide a cleaner test.

```python
// ... existing code ...
@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    # --- Setup Mocks ---

    # 1. Mock for the manual exclusion DB check (called directly by is_url_allowed)
    # This mock will be used for both calls to is_url_allowed in this test.
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) # Simulate domain is NOT manually excluded
    
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    
    # Configure the pool to return this connection when get_connection is called
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion

    # 2. Mock _get_robots_for_domain to return a pre-configured RERP instance
    # This prevents us from needing to mock the fetcher or _get_robots_for_domain's internal DB logic.
    rerp_instance = RobotExclusionRulesParser()
    rerp_instance.parse(robots_text)
    
    # Patch the method on the politeness_enforcer instance directly
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp_instance)

    # --- Test Allowed URL ---
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    
    # Verify interactions for the allowed_url call:
    # - Manual exclusion check should have happened
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    mock_conn_manual_exclusion.cursor.assert_called_once()
    mock_cursor_manual_exclusion.execute.assert_called_once() # Check query if needed, but existence is key
    mock_cursor_manual_exclusion.fetchone.assert_called_once()
    # - _get_robots_for_domain should have been called
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # --- Reset mocks for the next call ---
    politeness_enforcer.storage.db_pool.get_connection.reset_mock()
    mock_conn_manual_exclusion.cursor.reset_mock()
    mock_cursor_manual_exclusion.execute.reset_mock()
    mock_cursor_manual_exclusion.fetchone.reset_mock()
    politeness_enforcer._get_robots_for_domain.reset_mock()
    # Re-apply the same return value for manual exclusion check, as it might be called again
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) 
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion
    # The _get_robots_for_domain mock is still in place with its return_value

    # --- Test Disallowed URL ---
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False

    # Verify interactions for the disallowed_url call:
    # - Manual exclusion check
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    mock_conn_manual_exclusion.cursor.assert_called_once()
    # - _get_robots_for_domain should have been called again
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)
// ... existing code ...
```

---

**User**

I really feel like this test is "checking the implementation" rather than the interface. Why do we need to have all these assert_called_once type of things? Why can't we just verify the expected return value (url is allowed or not allowed) for the urls given the robots txt?

---

**Cursor**

You're articulating a very common and valid perspective in testing philosophy: **prefer testing state/behavior over implementation details.** The `assert_called_once` and similar assertions can indeed make tests more brittle if the underlying implementation changes while the overall behavior remains correct.

My intention with those detailed assertions was to ensure that the key collaboration points (like checking the database for manual exclusions, then fetching/using robots.txt) were happening as expected, especially after a refactor. However, for a test like `test_is_url_allowed_by_robots`, if we trust that:
1.  The manual exclusion check (which is a direct DB call we mock) works correctly.
2.  The `_get_robots_for_domain` method (which we also mock for this test) correctly returns a `RobotExclusionRulesParser` object.

Then, the primary concern of `test_is_url_allowed_by_robots` boils down to: "Given these inputs from its dependencies, does `is_url_allowed` combine them to produce the correct boolean output?"

You're right. We can simplify this specific test to focus more on that behavioral outcome, reducing the detailed assertions about *how* the manual exclusion check was performed via the pool, as long as its effect (not excluded) is achieved through the mock.

Let's revise `test_is_url_allowed_by_robots` to be less focused on the specific calls to `db_pool.get_connection().cursor().execute().fetchone()` for the manual exclusion part, and primarily ensure that `_get_robots_for_domain` is involved when appropriate.

**Revised `test_is_url_allowed_by_robots`:**

```python:crawler/tests/test_politeness.py
// ... existing code ...
@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    # --- Setup Mocks ---

    # 1. Mock for the manual exclusion DB check.
    # We need to ensure that when PolitenessEnforcer checks for manual exclusion,
    # it believes the domain is NOT excluded.
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) # Simulate domain is NOT manually excluded
    
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    
    # Configure the pool to return this connection for the manual exclusion check.
    # We'll assume is_url_allowed makes one call to get_connection for this.
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion

    # 2. Mock _get_robots_for_domain to return a pre-configured RERP instance.
    rerp_instance = RobotExclusionRulesParser()
    rerp_instance.parse(robots_text)
    
    # Patch the _get_robots_for_domain method on the instance.
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp_instance)

    # --- Test Allowed URL ---
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    # Verify _get_robots_for_domain was called, as it wasn't manually excluded.
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # --- Reset for next test case (if _get_robots_for_domain is stateful or for clarity) ---
    # Resetting the mock for _get_robots_for_domain as it's called per is_url_allowed.
    politeness_enforcer._get_robots_for_domain.reset_mock()
    # The get_connection mock for manual exclusion is reset implicitly if it's
    # set up per call or if we re-assign its return_value/side_effect.
    # For this simplified test, we assume the manual exclusion setup is consistent.
    # If `get_connection` was using a `side_effect` list, more careful reset would be needed.
    # Since it's a simple `return_value`, it will be used again for the next call.

    # --- Test Disallowed URL ---
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    # Verify _get_robots_for_domain was called again.
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # Optional: Verify that the DB pool was asked for a connection (for manual exclusion) at least once per call.
    # This is a minimal check that the path involving DB for exclusion was attempted.
    # For two calls to is_url_allowed, we'd expect two calls to get_connection for manual exclusion.
    assert politeness_enforcer.storage.db_pool.get_connection.call_count == 2
// ... existing code ...
```

**Changes in this version of `test_is_url_allowed_by_robots`:**

*   **Reduced Interaction Assertions for Manual Exclusion:** We've removed the specific `mock_conn_manual_exclusion.cursor.assert_called_once()` and `mock_cursor_manual_exclusion.execute.assert_called_once()`. The setup ensures that `fetchone()` returns `(0,)`, effectively making the manual exclusion check pass (i.e., domain is not excluded).
*   **Focus on `_get_robots_for_domain` Call:** We still assert that `_get_robots_for_domain` was called, because this is a critical part of `is_url_allowed`'s contract when a domain isn't manually excluded.
*   **Overall DB Pool Call Count:** Added an assertion at the end to check that `get_connection` was called twice on the pool (once for each `is_url_allowed` call, for their respective manual exclusion checks). This is a less granular way to confirm the DB interaction for exclusion happened, without detailing each cursor/execute.

This revised test still ensures:
1.  The final boolean output is correct based on the mocked dependencies.
2.  The key dependency `_get_robots_for_domain` is actually used.
3.  The manual exclusion check (which uses the DB pool) is engaged.

This strikes a better balance for this specific test, making it less about the exact sequence of `cursor().execute().fetchone()` for the exclusion, and more about the overall behavior and key collaborations.

The more detailed interaction mocking (checking `cursor`, `execute` specifically) is still valuable for tests that *directly target* methods performing those specific database operations, like the tests for `_get_robots_for_domain`'s caching logic or `_load_manual_exclusions`.

I will apply this simplified version of `test_is_url_allowed_by_robots`.

Regarding the linter error in `test_load_manual_exclusions_with_file`:
`crawler/tests/test_politeness.py:95: error: Item "None" of "Connection | None" has no attribute "cursor" [union-attr]`

This occurs at:
```python
        conn_verify: sqlite3.Connection | None = None # Line 94 in your previous snippet
        try:
            db_pool_to_check = storage_manager_for_exclusion_test.db_pool
            conn_verify = db_pool_to_check.get_connection() # This returns Connection or raises
            cursor = conn_verify.cursor() # Linter complains here
```
The `db_pool.get_connection()` method is typed to return `sqlite3.Connection` and raises `queue.Empty` on failure (it doesn't return `None`). The variable `conn_verify` is initialized with `| None` which might be what's confusing the type checker.

I'll adjust that specific part in `test_load_manual_exclusions_with_file` to reflect that `get_connection` is expected to return a connection or raise an exception.

```python:crawler/tests/test_politeness.py
// ... existing code ...
def test_load_manual_exclusions_with_file(
    dummy_config: CrawlerConfig, 
    storage_manager_for_exclusion_test: StorageManager,
    mock_fetcher: MagicMock,
    tmp_path: Path
):
    exclude_file_path = tmp_path / "custom_excludes.txt"
    dummy_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    pe = PolitenessEnforcer(config=dummy_config, storage=storage_manager_for_exclusion_test, fetcher=mock_fetcher)
    
    # conn_verify will be of type sqlite3.Connection, or get_connection would have raised.
    db_pool_to_check = storage_manager_for_exclusion_test.db_pool
    conn_verify: sqlite3.Connection = db_pool_to_check.get_connection()
    cursor_verify: sqlite3.Cursor | None = None # Initialize for finally block
    try:
        cursor_verify = conn_verify.cursor()
        
        expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
        for domain in expected_domains:
            cursor_verify.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", (domain,))
            row = cursor_verify.fetchone()
            assert row is not None, f"Domain {domain} should be in domain_metadata"
            assert row[0] == 1, f"Domain {domain} should be marked as manually excluded"
        
        cursor_verify.execute("SELECT COUNT(*) FROM domain_metadata WHERE is_manually_excluded = 1")
        count = cursor_verify.fetchone()[0]
        assert count == len(expected_domains)

    finally:
        if cursor_verify: cursor_verify.close()
        # Return the connection to the pool it came from
        db_pool_to_check.return_connection(conn_verify) 
        if exclude_file_path.exists(): exclude_file_path.unlink()

// ... existing code ...
@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    # --- Setup Mocks ---

    # 1. Mock for the manual exclusion DB check.
    # We need to ensure that when PolitenessEnforcer checks for manual exclusion,
    # it believes the domain is NOT excluded.
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) # Simulate domain is NOT manually excluded
    
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    
    # Configure the pool to return this connection for the manual exclusion check.
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion

    # 2. Mock _get_robots_for_domain to return a pre-configured RERP instance.
    rerp_instance = RobotExclusionRulesParser()
    rerp_instance.parse(robots_text)
    
    # Patch the _get_robots_for_domain method on the instance.
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp_instance)

    # --- Test Allowed URL ---
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    # Verify _get_robots_for_domain was called, as it wasn't manually excluded.
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # --- Reset for next test case ---
    politeness_enforcer._get_robots_for_domain.reset_mock()
    # The politeness_enforcer.storage.db_pool.get_connection mock is still set to return mock_conn_manual_exclusion
    # and mock_conn_manual_exclusion's cursor is still set to return (0,) for fetchone.
    # This is fine as we expect the same manual exclusion behavior for the next call.

    # --- Test Disallowed URL ---
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    # Verify _get_robots_for_domain was called again.
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # Check that the db_pool's get_connection was called for each is_url_allowed call (for manual exclusion)
    assert politeness_enforcer.storage.db_pool.get_connection.call_count == 2

// ... existing code ...
@pytest.mark.asyncio
async def test_is_url_allowed_robots_fetch_fails_defaults_to_allow(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsfail.com"
    url = f"http://{domain}/anypage"

    # 1. Mock for manual exclusion: not excluded
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,)
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion
    
    # 2. Mock _get_robots_for_domain to simulate it returning None (e.g., all fetch attempts failed)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=None)
    
    # Ensure no in-memory RERP for this domain
    politeness_enforcer.robots_parsers.pop(domain, None)

    # The actual fetcher calls within the real _get_robots_for_domain are now bypassed by mocking _get_robots_for_domain itself.
    # So, we no longer need to mock politeness_enforcer.fetcher.fetch_url for this specific test's purpose.
    
    assert await politeness_enforcer.is_url_allowed(url) is True 
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once() # For manual exclusion
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

// ... existing code ...
@pytest.mark.asyncio
async def test_can_fetch_domain_now_insufficient_delay(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "notready.com"
    crawl_delay_val = MIN_CRAWL_DELAY_SECONDS
    last_fetch = int(time.time()) - int(crawl_delay_val / 2) # Fetched half the delay time ago

    # Mock the DB call for getting last_fetch_time
    mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor.fetchone.return_value = (last_fetch,)
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_conn.cursor.return_value = mock_db_cursor
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn
    
    # Mock get_crawl_delay to return a known value
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=crawl_delay_val)

    assert await politeness_enforcer.can_fetch_domain_now(domain) is False
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once() # For last_fetch_time
    politeness_enforcer.get_crawl_delay.assert_called_once_with(domain)


@pytest.mark.asyncio
async def test_can_fetch_domain_now_db_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "dbfetcherror.com"
    
    # Simulate DB error when trying to get a connection or during execute
    politeness_enforcer.storage.db_pool.get_connection.side_effect = sqlite3.Error("Simulated DB error getting connection")
    
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(MIN_CRAWL_DELAY_SECONDS))

    assert await politeness_enforcer.can_fetch_domain_now(domain) is False
    # get_crawl_delay might or might not be called depending on when the DB error is raised.
    # If get_connection fails, get_crawl_delay won't be called.
    # Let's assert get_connection was attempted.
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    politeness_enforcer.get_crawl_delay.assert_not_called() # Because DB error should happen first

// ... existing code ...
@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_db_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "recorddberror.com"
    
    # Simulate DB error during the get_connection call for recording fetch attempt
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_conn.cursor.side_effect = sqlite3.Error("Simulated DB error on cursor") # or .commit.side_effect
    # Alternatively, make get_connection itself fail:
    politeness_enforcer.storage.db_pool.get_connection.side_effect = sqlite3.Error("Simulated DB error getting connection")

    await politeness_enforcer.record_domain_fetch_attempt(domain)
    
    # Verify that get_connection was attempted
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    # If get_connection fails, no commit would be called on a connection object from the pool.
    # If get_connection succeeds but cursor/commit fails, then the mock_db_conn.commit might be relevant.
    # Given the side_effect on get_connection, the latter won't happen.
```

I've applied the simplified interaction testing to `test_is_url_allowed_by_robots`.
I've also adjusted `test_load_manual_exclusions_with_file` to correctly type `conn_verify` and initialize `cursor_verify` to satisfy the linter and better reflect expectations.
Additionally, I've started refactoring a few other tests (`test_is_url_allowed_robots_fetch_fails_defaults_to_allow`, `test_can_fetch_domain_now_insufficient_delay`, `test_can_fetch_domain_now_db_error`, `test_record_domain_fetch_attempt_db_error`) to use the new mocking strategy targeting `db_pool.get_connection` rather than the old `storage.conn` pattern.

This is still an iterative process. The remaining tests that interact with the database via `PolitenessEnforcer` will need similar adjustments. The core idea is to mock `politeness_enforcer.storage.db_pool.get_connection()` to control what the database "returns" for each specific DB operation within the method being tested. If a method makes multiple distinct DB calls, `get_connection.side_effect` can be a list of mock connections.

```python
// ... existing code ...
@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    # --- Setup Mocks ---

    # 1. Mock for the manual exclusion DB check.
    # We need to ensure that when PolitenessEnforcer checks for manual exclusion,
    # it believes the domain is NOT excluded.
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) # Simulate domain is NOT manually excluded
    
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    
    # Configure the pool to return this connection for the manual exclusion check.
    # We'll assume is_url_allowed makes one call to get_connection for this.
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion

    # 2. Mock _get_robots_for_domain to return a pre-configured RERP instance.
    rerp_instance = RobotExclusionRulesParser()
    rerp_instance.parse(robots_text)
    
    # Patch the _get_robots_for_domain method on the instance.
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp_instance)

    # --- Test Allowed URL ---
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    # Verify _get_robots_for_domain was called, as it wasn't manually excluded.
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # --- Reset for next test case (if _get_robots_for_domain is stateful or for clarity) ---
    # Resetting the mock for _get_robots_for_domain as it's called per is_url_allowed.
    politeness_enforcer._get_robots_for_domain.reset_mock()
    # The get_connection mock for manual exclusion is reset implicitly if it's
    # set up per call or if we re-assign its return_value/side_effect.
    # For this simplified test, we assume the manual exclusion setup is consistent.
    # If `get_connection` was using a `side_effect` list, more careful reset would be needed.
    # Since it's a simple `return_value`, it will be used again for the next call.

    # --- Test Disallowed URL ---
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    # Verify _get_robots_for_domain was called again.
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # Optional: Verify that the DB pool was asked for a connection (for manual exclusion) at least once per call.
    # This is a minimal check that the path involving DB for exclusion was attempted.
    # For two calls to is_url_allowed, we'd expect two calls to get_connection for manual exclusion.
    assert politeness_enforcer.storage.db_pool.get_connection.call_count == 2
// ... existing code ...
```

```python
        conn_verify: sqlite3.Connection | None = None # Line 94 in your previous snippet
        try:
            db_pool_to_check = storage_manager_for_exclusion_test.db_pool
            conn_verify = db_pool_to_check.get_connection() # This returns Connection or raises
            cursor = conn_verify.cursor() # Linter complains here
```

```python
// ... existing code ...
def test_load_manual_exclusions_with_file(
    dummy_config: CrawlerConfig, 
    storage_manager_for_exclusion_test: StorageManager,
    mock_fetcher: MagicMock,
    tmp_path: Path
):
    exclude_file_path = tmp_path / "custom_excludes.txt"
    dummy_config.exclude_file = exclude_file_path
    
    mock_file_content = "excluded1.com\n#comment\nexcluded2.com\n   excluded3.com  \n"
    
    with open(exclude_file_path, 'w') as f:
        f.write(mock_file_content)

    pe = PolitenessEnforcer(config=dummy_config, storage=storage_manager_for_exclusion_test, fetcher=mock_fetcher)
    
    # conn_verify will be of type sqlite3.Connection, or get_connection would have raised.
    db_pool_to_check = storage_manager_for_exclusion_test.db_pool
    conn_verify: sqlite3.Connection = db_pool_to_check.get_connection()
    cursor_verify: sqlite3.Cursor | None = None # Initialize for finally block
    try:
        cursor_verify = conn_verify.cursor()
        
        expected_domains = ["excluded1.com", "excluded2.com", "excluded3.com"]
        for domain in expected_domains:
            cursor_verify.execute("SELECT is_manually_excluded FROM domain_metadata WHERE domain = ?", (domain,))
            row = cursor_verify.fetchone()
            assert row is not None, f"Domain {domain} should be in domain_metadata"
            assert row[0] == 1, f"Domain {domain} should be marked as manually excluded"
        
        cursor_verify.execute("SELECT COUNT(*) FROM domain_metadata WHERE is_manually_excluded = 1")
        count = cursor_verify.fetchone()[0]
        assert count == len(expected_domains)

    finally:
        if cursor_verify: cursor_verify.close()
        # Return the connection to the pool it came from
        db_pool_to_check.return_connection(conn_verify) 
        if exclude_file_path.exists(): exclude_file_path.unlink()

// ... existing code ...
@pytest.mark.asyncio
async def test_is_url_allowed_by_robots(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsallowed.com"
    allowed_url = f"http://{domain}/allowed"
    disallowed_url = f"http://{domain}/disallowed"
    robots_text = f"User-agent: {dummy_config.user_agent}\nDisallow: /disallowed"

    # --- Setup Mocks ---

    # 1. Mock for the manual exclusion DB check.
    # We need to ensure that when PolitenessEnforcer checks for manual exclusion,
    # it believes the domain is NOT excluded.
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,) # Simulate domain is NOT manually excluded
    
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    
    # Configure the pool to return this connection for the manual exclusion check.
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion

    # 2. Mock _get_robots_for_domain to return a pre-configured RERP instance.
    rerp_instance = RobotExclusionRulesParser()
    rerp_instance.parse(robots_text)
    
    # Patch the _get_robots_for_domain method on the instance.
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=rerp_instance)

    # --- Test Allowed URL ---
    assert await politeness_enforcer.is_url_allowed(allowed_url) is True
    # Verify _get_robots_for_domain was called, as it wasn't manually excluded.
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # --- Reset for next test case ---
    politeness_enforcer._get_robots_for_domain.reset_mock()
    # The politeness_enforcer.storage.db_pool.get_connection mock is still set to return mock_conn_manual_exclusion
    # and mock_conn_manual_exclusion's cursor is still set to return (0,) for fetchone.
    # This is fine as we expect the same manual exclusion behavior for the next call.

    # --- Test Disallowed URL ---
    assert await politeness_enforcer.is_url_allowed(disallowed_url) is False
    # Verify _get_robots_for_domain was called again.
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

    # Check that the db_pool's get_connection was called for each is_url_allowed call (for manual exclusion)
    assert politeness_enforcer.storage.db_pool.get_connection.call_count == 2

// ... existing code ...
@pytest.mark.asyncio
async def test_is_url_allowed_robots_fetch_fails_defaults_to_allow(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "robotsfail.com"
    url = f"http://{domain}/anypage"

    # 1. Mock for manual exclusion: not excluded
    mock_cursor_manual_exclusion = MagicMock(spec=sqlite3.Cursor)
    mock_cursor_manual_exclusion.fetchone.return_value = (0,)
    mock_conn_manual_exclusion = MagicMock(spec=sqlite3.Connection)
    mock_conn_manual_exclusion.cursor.return_value = mock_cursor_manual_exclusion
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_conn_manual_exclusion
    
    # 2. Mock _get_robots_for_domain to simulate it returning None (e.g., all fetch attempts failed)
    politeness_enforcer._get_robots_for_domain = AsyncMock(return_value=None)
    
    # Ensure no in-memory RERP for this domain
    politeness_enforcer.robots_parsers.pop(domain, None)

    # The actual fetcher calls within the real _get_robots_for_domain are now bypassed by mocking _get_robots_for_domain itself.
    # So, we no longer need to mock politeness_enforcer.fetcher.fetch_url for this specific test's purpose.
    
    assert await politeness_enforcer.is_url_allowed(url) is True 
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once() # For manual exclusion
    politeness_enforcer._get_robots_for_domain.assert_called_once_with(domain)

// ... existing code ...
@pytest.mark.asyncio
async def test_can_fetch_domain_now_insufficient_delay(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "notready.com"
    crawl_delay_val = MIN_CRAWL_DELAY_SECONDS
    last_fetch = int(time.time()) - int(crawl_delay_val / 2) # Fetched half the delay time ago

    # Mock the DB call for getting last_fetch_time
    mock_db_cursor = MagicMock(spec=sqlite3.Cursor)
    mock_db_cursor.fetchone.return_value = (last_fetch,)
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_conn.cursor.return_value = mock_db_cursor
    politeness_enforcer.storage.db_pool.get_connection.return_value = mock_db_conn
    
    # Mock get_crawl_delay to return a known value
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=crawl_delay_val)

    assert await politeness_enforcer.can_fetch_domain_now(domain) is False
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once() # For last_fetch_time
    politeness_enforcer.get_crawl_delay.assert_called_once_with(domain)


@pytest.mark.asyncio
async def test_can_fetch_domain_now_db_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "dbfetcherror.com"
    
    # Simulate DB error when trying to get a connection or during execute
    politeness_enforcer.storage.db_pool.get_connection.side_effect = sqlite3.Error("Simulated DB error getting connection")
    
    politeness_enforcer.get_crawl_delay = AsyncMock(return_value=float(MIN_CRAWL_DELAY_SECONDS))

    assert await politeness_enforcer.can_fetch_domain_now(domain) is False
    # get_crawl_delay might or might not be called depending on when the DB error is raised.
    # If get_connection fails, get_crawl_delay won't be called.
    # Let's assert get_connection was attempted.
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    politeness_enforcer.get_crawl_delay.assert_not_called() # Because DB error should happen first

// ... existing code ...
@pytest.mark.asyncio
async def test_record_domain_fetch_attempt_db_error(politeness_enforcer: PolitenessEnforcer, dummy_config: CrawlerConfig):
    domain = "recorddberror.com"
    
    # Simulate DB error during the get_connection call for recording fetch attempt
    mock_db_conn = MagicMock(spec=sqlite3.Connection)
    mock_db_conn.cursor.side_effect = sqlite3.Error("Simulated DB error on cursor") # or .commit.side_effect
    # Alternatively, make get_connection itself fail:
    politeness_enforcer.storage.db_pool.get_connection.side_effect = sqlite3.Error("Simulated DB error getting connection")

    await politeness_enforcer.record_domain_fetch_attempt(domain)
    
    # Verify that get_connection was attempted
    politeness_enforcer.storage.db_pool.get_connection.assert_called_once()
    # If get_connection fails, no commit would be called on a connection object from the pool.
    # If get_connection succeeds but cursor/commit fails, then the mock_db_conn.commit might be relevant.
    # Given the side_effect on get_connection, the latter won't happen.
```

---

**User**

great! now we just need to fixup test_storage.py:


---

**Cursor**

You got it. We'll pause on `test_politeness.py` for now and switch focus to `crawler/tests/test_storage.py`.

Based on our previous discussion and the changes made (introducing `SQLiteConnectionPool` and modifying `StorageManager` to use it), the tests in `test_storage.py` need the following adjustments:

1.  The `storage_manager` fixture needs to accept the `db_pool` fixture (from `conftest.py`) and pass it to the `StorageManager` constructor.
2.  Tests that directly instantiated `StorageManager` or need to inspect the database state will require access to the `db_pool`.
3.  Database verification should use connections obtained from the `db_pool`.

Here are the updates for `crawler/tests/test_storage.py`:

```python:crawler/tests/test_storage.py
import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

# Import db_pool from conftest
# pytest will automatically discover fixtures in conftest.py in the same directory or parent directories.

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig 
from crawler_module.db_pool import SQLiteConnectionPool # For test_storage_manager_reinitialization

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@pytest.fixture
def temp_test_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for test data specific to storage tests."""
    test_data_dir = tmp_path / "test_crawler_data_storage_specific"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir for storage tests: {test_data_dir}")
    return test_data_dir

@pytest.fixture
def dummy_config_for_storage(temp_test_dir: Path) -> CrawlerConfig: # Renamed to avoid conflict if generic dummy_config exists
    """Create a dummy CrawlerConfig tailored for testing StorageManager."""
    seed_file_path = temp_test_dir / "dummy_seeds_storage.txt"
    # Ensure seed file exists if any component might try to read it, even if not used by StorageManager directly
    if not seed_file_path.exists():
        seed_file_path.write_text("http://example.com/storage_seed\n")
    
    cfg = CrawlerConfig(
        data_dir=temp_test_dir, # Use the specific temp dir for storage tests
        seed_file=seed_file_path,
        # Use default values from CrawlerConfig or provide specific ones for storage tests
        email="storage_test@example.com",
        exclude_file=None,
        max_workers=1,
        max_pages=None, # Or a small number if relevant
        max_duration=None, # Or a small number
        log_level="DEBUG",
        resume=False,
        user_agent="StorageTestCrawler/1.0",
        seeded_urls_only=False
    )
    logger.debug(f"Created dummy_config_for_storage with data_dir: {cfg.data_dir}")
    return cfg

# Updated storage_manager fixture in test_storage.py
# It uses the db_pool fixture from conftest.py
@pytest.fixture
def storage_manager(dummy_config_for_storage: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {dummy_config_for_storage.data_dir} and db_pool: {db_pool.db_path}")
    sm = StorageManager(config=dummy_config_for_storage, db_pool=db_pool)
    yield sm
    # Pool teardown is handled by the db_pool fixture itself.

def test_storage_manager_initialization(storage_manager: StorageManager, dummy_config_for_storage: CrawlerConfig, db_pool: SQLiteConnectionPool):
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    # The DB file itself is managed by the pool. Its path is db_pool.db_path.
    # storage_manager.db_path refers to the *expected* path based on config, which db_pool should use.
    assert Path(db_pool.db_path).exists(), f"Database file {db_pool.db_path} should exist."
    assert Path(db_pool.db_path) == storage_manager.db_path, "StorageManager's db_path should match pool's db_path."
    
    # Verify schema initialization by checking table presence
    with db_pool as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';")
        assert cursor.fetchone() is not None, "schema_version table should exist."
        cursor.close()
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test if all tables and indexes are created as expected using a connection from the pool."""
    # The storage_manager fixture already initializes the schema via StorageManager.__init__
    with db_pool as conn: # Use the pool's context manager to get a connection
        cursor = conn.cursor()

        tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
        for table in tables_to_check:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            assert cursor.fetchone() is not None, f"Table {table} should exist."

        cursor.execute("SELECT version FROM schema_version")
        assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
        assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
        assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
        
        cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager):
    """Test the URL hashing function."""
    url1 = "http://example.com/page1"
    url2 = "http://example.com/page2"
    url1_again = "http://example.com/page1"

    hash1 = storage_manager.get_url_sha256(url1)
    hash2 = storage_manager.get_url_sha256(url2)
    hash1_again = storage_manager.get_url_sha256(url1_again)

    assert isinstance(hash1, str)
    assert len(hash1) == 64 # SHA256 hex digest length
    assert hash1 != hash2
    assert hash1 == hash1_again
    logger.info("URL SHA256 hashing test passed.")

def test_storage_manager_reinitialization(dummy_config_for_storage: CrawlerConfig, tmp_path: Path):
    """Test that re-initializing StorageManager with the same path is safe.
       This test needs its own pool instances to simulate re-initialization.
    """
    # Use a consistent db file path within the test-specific tmp_path
    db_file = tmp_path / "reinit_test_crawler_state.db"
    # Ensure the config's data_dir points to where this db_file will reside,
    # or ensure the pool uses an absolute path to db_file.
    # dummy_config_for_storage.data_dir is already tmp_path / "test_crawler_data_storage_specific"
    # So we'll adjust db_file to be within that.
    
    config_for_reinit = dummy_config_for_storage 
    # Ensure the db_file path is consistent with the config's data_dir for StorageManager
    db_file_for_this_test = config_for_reinit.data_dir / "reinit_specific.db"

    pool1 = SQLiteConnectionPool(db_path=db_file_for_this_test, pool_size=1)
    sm1 = StorageManager(config=config_for_reinit, db_pool=pool1)
    # _init_db_schema is called by StorageManager.__init__
    db_path_from_sm1 = sm1.db_path
    pool1.close_all() # Close pool after first SM is done

    # Create a new pool and SM instance pointing to the *same* database file
    pool2 = SQLiteConnectionPool(db_path=db_file_for_this_test, pool_size=1)
    sm2 = StorageManager(config=config_for_reinit, db_pool=pool2)
    db_path_from_sm2 = sm2.db_path
    pool2.close_all() # Close pool after second SM is done

    assert db_path_from_sm1 == db_file_for_this_test
    assert db_path_from_sm2 == db_file_for_this_test
    assert db_file_for_this_test.exists(), "Database file should exist after StorageManager initializations."
    logger.info("StorageManager re-initialization test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager): # Removed tmp_path, uses SM's content_dir
    """Test saving content to a file asynchronously."""
    url_hash = "test_url_hash_content"
    text_content = "This is some test content.\nWith multiple lines."
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is not None
    assert file_path.exists()
    assert file_path.name == f"{url_hash}.txt"
    assert file_path.parent == storage_manager.content_dir

    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
        saved_content = await f.read()
    assert saved_content == text_content
    logger.info("Save content to file test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
    """Test that saving empty content returns None and creates no file."""
    url_hash = "empty_content_hash"
    text_content = ""
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    # Ensure no file was created
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save empty content to file test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
    """Test handling of IOError during file save."""
    url_hash = "io_error_hash"
    text_content = "Some content that will fail to save."

    # Mock aiofiles.open to raise IOError
    async def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Simulated disk full error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save content to file with IOError test passed.")

def test_add_visited_page(storage_manager: StorageManager, db_pool: SQLiteConnectionPool): # dummy_config_for_storage implicitly used by sm
    """Test adding a visited page record to the database."""
    url = "http://example.com/visited_page"
    domain = "example.com"
    status_code = 200
    crawled_timestamp = int(time.time())
    content_type = "text/html"
    text_content = "<html><body>Visited!</body></html>"
    # Simulate content saved to file
    url_hash_for_file = storage_manager.get_url_sha256(url)
    # For this test, we don't need to actually save the file, just provide a path.
    content_storage_path_str = str(storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    # add_visited_page itself is synchronous and uses to_thread internally if it were async,
    # but its DB ops are now via the pool.
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_text=text_content, # For content_hash generation
        content_storage_path_str=content_storage_path_str,
        redirected_to_url=None
    )

    # Verify the record in the database using a connection from the pool
    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

        cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    (db_url_sha256, db_url, db_domain, db_crawled_ts, db_status, 
     db_content_type, db_content_hash, db_storage_path, db_redirected_to) = row
    
    assert db_url_sha256 == expected_url_sha256
    assert db_url == url
    assert db_domain == domain
    assert db_crawled_ts == crawled_timestamp
    assert db_status == status_code
    assert db_content_type == content_type
    assert db_content_hash == expected_content_hash
    assert db_storage_path == content_storage_path_str
    assert db_redirected_to is None
    logger.info("Add visited page test passed.")

def test_add_visited_page_no_content(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
    url = "http://example.com/redirect"
    domain = "example.com"
    status_code = 301
    crawled_timestamp = int(time.time())
    redirected_to_url = "http://example.com/final_destination"

    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=None,
        content_text=None,
        content_storage_path_str=None,
        redirected_to_url=redirected_to_url
    )

    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    content_hash, content_storage_path, db_redirected_to = row
    assert content_hash is None
    assert content_storage_path is None
    assert db_redirected_to == redirected_to_url
    logger.info("Add visited page with no content test passed.")
```

**Key changes made:**

1.  **`dummy_config_for_storage`**: Renamed the config fixture to be specific to this test file to avoid potential conflicts if a general `dummy_config` exists or is added later in `conftest.py`. Ensured its `data_dir` is distinct.
2.  **`storage_manager` fixture**:
    *   Takes `dummy_config_for_storage` and `db_pool` (from `conftest.py`) as arguments.
    *   Passes `db_pool` to `StorageManager(config=dummy_config_for_storage, db_pool=db_pool)`.
3.  **`test_storage_manager_initialization`**:
    *   Asserts `Path(db_pool.db_path).exists()` and `storage_manager.db_path == Path(db_pool.db_path)`.
    *   Verifies schema initialization via the pool.
4.  **`test_database_schema_creation`**:
    *   Uses `with db_pool as conn:` to get a connection for verifying table and index creation.
5.  **`test_storage_manager_reinitialization`**:
    *   Now correctly creates two separate `SQLiteConnectionPool` instances (`pool1`, `pool2`) pointing to the *same specific database file path* (`db_file_for_this_test`).
    *   Passes these distinct pools to the two `StorageManager` instances.
    *   Calls `pool1.close_all()` and `pool2.close_all()` for cleanup.
6.  **`test_add_visited_page` and `test_add_visited_page_no_content`**:
    *   Accept `db_pool` as an argument.
    *   Use `with db_pool as conn:` to get a connection for verifying records in the `visited_urls` table.

These changes should align `test_storage.py` with the new `StorageManager` design using the connection pool.

After applying these, you can re-run your tests. If `test_storage.py` passes, we can then systematically go through the remaining failing tests in `test_frontier.py` and `test_politeness.py`.

```python
import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

# Import db_pool from conftest
# pytest will automatically discover fixtures in conftest.py in the same directory or parent directories.

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig 
from crawler_module.db_pool import SQLiteConnectionPool # For test_storage_manager_reinitialization

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

@pytest.fixture
def temp_test_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for test data specific to storage tests."""
    test_data_dir = tmp_path / "test_crawler_data_storage_specific"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created temp test dir for storage tests: {test_data_dir}")
    return test_data_dir

@pytest.fixture
def dummy_config_for_storage(temp_test_dir: Path) -> CrawlerConfig: # Renamed to avoid conflict if generic dummy_config exists
    """Create a dummy CrawlerConfig tailored for testing StorageManager."""
    seed_file_path = temp_test_dir / "dummy_seeds_storage.txt"
    # Ensure seed file exists if any component might try to read it, even if not used by StorageManager directly
    if not seed_file_path.exists():
        seed_file_path.write_text("http://example.com/storage_seed\n")
    
    cfg = CrawlerConfig(
        data_dir=temp_test_dir, # Use the specific temp dir for storage tests
        seed_file=seed_file_path,
        # Use default values from CrawlerConfig or provide specific ones for storage tests
        email="storage_test@example.com",
        exclude_file=None,
        max_workers=1,
        max_pages=None, # Or a small number if relevant
        max_duration=None, # Or a small number
        log_level="DEBUG",
        resume=False,
        user_agent="StorageTestCrawler/1.0",
        seeded_urls_only=False
    )
    logger.debug(f"Created dummy_config_for_storage with data_dir: {cfg.data_dir}")
    return cfg

# Updated storage_manager fixture in test_storage.py
# It uses the db_pool fixture from conftest.py
@pytest.fixture
def storage_manager(dummy_config_for_storage: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {dummy_config_for_storage.data_dir} and db_pool: {db_pool.db_path}")
    sm = StorageManager(config=dummy_config_for_storage, db_pool=db_pool)
    yield sm
    # Pool teardown is handled by the db_pool fixture itself.

def test_storage_manager_initialization(storage_manager: StorageManager, dummy_config_for_storage: CrawlerConfig, db_pool: SQLiteConnectionPool):
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    # The DB file itself is managed by the pool. Its path is db_pool.db_path.
    # storage_manager.db_path refers to the *expected* path based on config, which db_pool should use.
    assert Path(db_pool.db_path).exists(), f"Database file {db_pool.db_path} should exist."
    assert Path(db_pool.db_path) == storage_manager.db_path, "StorageManager's db_path should match pool's db_path."
    
    # Verify schema initialization by checking table presence
    with db_pool as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';")
        assert cursor.fetchone() is not None, "schema_version table should exist."
        cursor.close()
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test if all tables and indexes are created as expected using a connection from the pool."""
    # The storage_manager fixture already initializes the schema via StorageManager.__init__
    with db_pool as conn: # Use the pool's context manager to get a connection
        cursor = conn.cursor()

        tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
        for table in tables_to_check:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            assert cursor.fetchone() is not None, f"Table {table} should exist."

        cursor.execute("SELECT version FROM schema_version")
        assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
        assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
        assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
        
        cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager):
    """Test the URL hashing function."""
    url1 = "http://example.com/page1"
    url2 = "http://example.com/page2"
    url1_again = "http://example.com/page1"

    hash1 = storage_manager.get_url_sha256(url1)
    hash2 = storage_manager.get_url_sha256(url2)
    hash1_again = storage_manager.get_url_sha256(url1_again)

    assert isinstance(hash1, str)
    assert len(hash1) == 64 # SHA256 hex digest length
    assert hash1 != hash2
    assert hash1 == hash1_again
    logger.info("URL SHA256 hashing test passed.")

def test_storage_manager_reinitialization(dummy_config_for_storage: CrawlerConfig, tmp_path: Path):
    """Test that re-initializing StorageManager with the same path is safe.
       This test needs its own pool instances to simulate re-initialization.
    """
    # Use a consistent db file path within the test-specific tmp_path
    db_file = tmp_path / "reinit_test_crawler_state.db"
    # Ensure the config's data_dir points to where this db_file will reside,
    # or ensure the pool uses an absolute path to db_file.
    # dummy_config_for_storage.data_dir is already tmp_path / "test_crawler_data_storage_specific"
    # So we'll adjust db_file to be within that.
    
    config_for_reinit = dummy_config_for_storage 
    # Ensure the db_file path is consistent with the config's data_dir for StorageManager
    db_file_for_this_test = config_for_reinit.data_dir / "reinit_specific.db"

    pool1 = SQLiteConnectionPool(db_path=db_file_for_this_test, pool_size=1)
    sm1 = StorageManager(config=config_for_reinit, db_pool=pool1)
    # _init_db_schema is called by StorageManager.__init__
    db_path_from_sm1 = sm1.db_path
    pool1.close_all() # Close pool after first SM is done

    # Create a new pool and SM instance pointing to the *same* database file
    pool2 = SQLiteConnectionPool(db_path=db_file_for_this_test, pool_size=1)
    sm2 = StorageManager(config=config_for_reinit, db_pool=pool2)
    db_path_from_sm2 = sm2.db_path
    pool2.close_all() # Close pool after second SM is done

    assert db_path_from_sm1 == db_file_for_this_test
    assert db_path_from_sm2 == db_file_for_this_test
    assert db_file_for_this_test.exists(), "Database file should exist after StorageManager initializations."
    logger.info("StorageManager re-initialization test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager): # Removed tmp_path, uses SM's content_dir
    """Test saving content to a file asynchronously."""
    url_hash = "test_url_hash_content"
    text_content = "This is some test content.\nWith multiple lines."
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is not None
    assert file_path.exists()
    assert file_path.name == f"{url_hash}.txt"
    assert file_path.parent == storage_manager.content_dir

    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
        saved_content = await f.read()
    assert saved_content == text_content
    logger.info("Save content to file test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
    """Test that saving empty content returns None and creates no file."""
    url_hash = "empty_content_hash"
    text_content = ""
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    # Ensure no file was created
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save empty content to file test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
    """Test handling of IOError during file save."""
    url_hash = "io_error_hash"
    text_content = "Some content that will fail to save."

    # Mock aiofiles.open to raise IOError
    async def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Simulated disk full error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save content to file with IOError test passed.")

def test_add_visited_page(storage_manager: StorageManager, db_pool: SQLiteConnectionPool): # dummy_config_for_storage implicitly used by sm
    """Test adding a visited page record to the database."""
    url = "http://example.com/visited_page"
    domain = "example.com"
    status_code = 200
    crawled_timestamp = int(time.time())
    content_type = "text/html"
    text_content = "<html><body>Visited!</body></html>"
    # Simulate content saved to file
    url_hash_for_file = storage_manager.get_url_sha256(url)
    # For this test, we don't need to actually save the file, just provide a path.
    content_storage_path_str = str(storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    # add_visited_page itself is synchronous and uses to_thread internally if it were async,
    # but its DB ops are now via the pool.
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_text=text_content, # For content_hash generation
        content_storage_path_str=content_storage_path_str,
        redirected_to_url=None
    )

    # Verify the record in the database using a connection from the pool
    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

        cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    (db_url_sha256, db_url, db_domain, db_crawled_ts, db_status, 
     db_content_type, db_content_hash, db_storage_path, db_redirected_to) = row
    
    assert db_url_sha256 == expected_url_sha256
    assert db_url == url
    assert db_domain == domain
    assert db_crawled_ts == crawled_timestamp
    assert db_status == status_code
    assert db_content_type == content_type
    assert db_content_hash == expected_content_hash
    assert db_storage_path == content_storage_path_str
    assert db_redirected_to is None
    logger.info("Add visited page test passed.")

def test_add_visited_page_no_content(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
    url = "http://example.com/redirect"
    domain = "example.com"
    status_code = 301
    crawled_timestamp = int(time.time())
    redirected_to_url = "http://example.com/final_destination"

    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=None,
        content_text=None,
        content_storage_path_str=None,
        redirected_to_url=redirected_to_url
    )

    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    content_hash, content_storage_path, db_redirected_to = row
    assert content_hash is None
    assert content_storage_path is None
    assert db_redirected_to == redirected_to_url
    logger.info("Add visited page with no content test passed.")
```

Okay, it looks like we have two failures in `test_storage.py` after the latest changes:

1.  **`test_storage_manager_initialization`**:
    *   `AssertionError: StorageManager's db_path should match pool's db_path.`
    *   This means `Path(db_pool.db_path)` is not equal to `storage_manager.db_path`.

2.  **`test_storage_manager_reinitialization`**:
    *   `AssertionError: assert PosixPath('/tmp/pytest-of-ubuntu/pytest-4/test_storage_manager_reinitial0/test_crawler_data_sto...` (The full path is truncated, but it's a path comparison).
    *   This usually happens when an expected path doesn't match an actual path.

Let's analyze these.

**Failure 1: `test_storage_manager_initialization`**

*   `storage_manager.db_path` is determined by `Path(config.data_dir) / "crawler_state.db"`.
*   `db_pool.db_path` is determined in the `conftest.py` `db_pool` fixture as `tmp_path / "test_crawler_state.db"`.

The issue is that `config.data_dir` (from `dummy_config_for_storage`) is `temp_test_dir` which is `tmp_path / "test_crawler_data_storage_specific"`.
And `db_pool` fixture in `conftest.py` uses `tmp_path` directly.

So:
*   `storage_manager.db_path` is `tmp_path / "test_crawler_data_storage_specific" / "crawler_state.db"`
*   `db_pool.db_path` is `tmp_path / "test_crawler_state.db"`

These are indeed different paths. The `StorageManager` is being initialized with a config that points to one data directory, but the `db_pool` fixture is creating its database in a different location (`tmp_path` instead of `config.data_dir`).

**Fix for `test_storage_manager_initialization`:**
The `db_pool` fixture in `conftest.py` should create its database file *relative to the `data_dir` specified in the configuration it will be used with*.

We need to make the `db_pool` fixture aware of the `test_config`'s `data_dir`.
We can modify the `db_pool` fixture in `conftest.py` to accept `test_config` and use its `data_dir`.

```python:crawler/conftest.py
// ... existing code ...
@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig: # This fixture should come before db_pool if db_pool depends on it
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data" # This is the general test_data dir
    data_dir.mkdir(parents=True, exist_ok=True) 
    
    seed_file = data_dir / "seeds.txt"
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")

    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir, # This data_dir will be used by StorageManager via this config
        max_workers=1, 
        log_level="DEBUG",
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False,
    )

@pytest.fixture(scope="function")
def db_pool(test_config: CrawlerConfig) -> Iterator[SQLiteConnectionPool]: # Now depends on test_config
    """
    Pytest fixture for a SQLiteConnectionPool.
    Creates the DB inside the test_config.data_dir.
    Ensures the pool is closed after the test.
    """
    # The database file should be created based on the config's data_dir
    # This ensures StorageManager and the pool point to the same DB location.
    db_file = Path(test_config.data_dir) / "crawler_state.db" 
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True) 
    yield pool 
    pool.close_all() 

@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
// ... existing code ...
```
By making `db_pool` depend on `test_config`, it will use the *same* `data_dir` that `StorageManager` uses when it's initialized with `test_config`. This should make the paths match.

Note: The `dummy_config_for_storage` fixture in `test_storage.py` creates *another* `CrawlerConfig` with a *different* `data_dir` (`tmp_path / "test_crawler_data_storage_specific"`). If `test_storage.py` uses its own `dummy_config_for_storage` with the `db_pool` from `conftest.py`, this mismatch will persist.

To resolve this, either:
a) `test_storage.py` should use the `test_config` from `conftest.py`.
b) The `db_pool` fixture in `conftest.py` needs to be more flexible or `test_storage.py` needs its *own* `db_pool` fixture that uses `dummy_config_for_storage.data_dir`.

Let's try option (a) first for simplicity, assuming `test_config` from `conftest.py` is suitable for `test_storage.py`. This means `test_storage.py` will no longer use `dummy_config_for_storage`.

```python:crawler/tests/test_storage.py
import pytest
from pathlib import Path
import sqlite3
// ... other imports ...
from crawler_module.config import CrawlerConfig # Will use test_config from conftest
from crawler_module.db_pool import SQLiteConnectionPool

# ... logger setup ...

# Remove temp_test_dir and dummy_config_for_storage if using conftest versions
# @pytest.fixture
# def temp_test_dir(tmp_path: Path) -> Path: ...
# @pytest.fixture
# def dummy_config_for_storage(temp_test_dir: Path) -> CrawlerConfig: ...

# storage_manager fixture now uses test_config from conftest.py
@pytest.fixture
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {test_config.data_dir} and db_pool: {db_pool.db_path}")
    sm = StorageManager(config=test_config, db_pool=db_pool)
    yield sm

# Test functions will now use 'test_config' if they need config directly
def test_storage_manager_initialization(storage_manager: StorageManager, test_config: CrawlerConfig, db_pool: SQLiteConnectionPool):
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    assert Path(db_pool.db_path).exists(), f"Database file {db_pool.db_path} should exist."
    # This assertion should now pass if db_pool uses test_config.data_dir
    assert storage_manager.db_path == Path(db_pool.db_path), \
        f"SM DB path {storage_manager.db_path} should match pool DB path {Path(db_pool.db_path)}"
    
    with db_pool as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';")
        assert cursor.fetchone() is not None, "schema_version table should exist."
        cursor.close()
    logger.info("StorageManager initialization test passed.")

# test_database_schema_creation should be fine as it uses storage_manager and db_pool

# test_get_url_sha256 is fine

def test_storage_manager_reinitialization(test_config: CrawlerConfig, tmp_path: Path): # Use test_config from conftest
    """Test that re-initializing StorageManager with the same path is safe."""
    
    # For this test, we want to ensure re-initialization on the *exact same DB file*.
    # The test_config.data_dir is from tmp_path/test_crawler_data.
    # We'll create a specific DB file within that for this test.
    db_file_for_this_test = Path(test_config.data_dir) / "reinit_specific_for_storage.db"

    # Create a config that will definitely use this data_dir for StorageManager's db_path calculation
    # The test_config passed in already has the correct data_dir.

    pool1 = SQLiteConnectionPool(db_path=db_file_for_this_test, pool_size=1)
    # StorageManager will calculate its db_path as test_config.data_dir / "crawler_state.db"
    # The pool is explicitly told to use db_file_for_this_test. These must align.
    # To make them align, StorageManager must be initialized with a config
    # such that config.data_dir / "crawler_state.db" == db_file_for_this_test
    # OR, the pool must use config.data_dir / "crawler_state.db".
    # Let's make the pool use the standard DB name within the config's data_dir.
    
    # For reinitialization test, we explicitly control the DB file.
    # The StorageManager will derive its expected DB path from its config.
    # The pool will use the path it's given.
    # The critical thing is that StorageManager's *operations* go to the pool's DB.
    
    # Let's make config for SM point to a dir, and the pool use a DB within that same dir.
    # test_config.data_dir is fine.
    # The standard DB name used by StorageManager is "crawler_state.db".
    # So, the pool for this test should also use that name within test_config.data_dir.
    
    target_db_path = Path(test_config.data_dir) / "crawler_state.db" # Standard path

    pool_reinit1 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm1 = StorageManager(config=test_config, db_pool=pool_reinit1)
    assert sm1.db_path == target_db_path, f"SM1 path {sm1.db_path} vs target {target_db_path}"
    pool_reinit1.close_all()

    pool_reinit2 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm2 = StorageManager(config=test_config, db_pool=pool_reinit2)
    assert sm2.db_path == target_db_path, f"SM2 path {sm2.db_path} vs target {target_db_path}"
    pool_reinit2.close_all()

    assert target_db_path.exists(), "Database file should exist after StorageManager initializations."
    logger.info("StorageManager re-initialization test passed.")

# Remaining tests use storage_manager fixture, which should now be correctly configured.
# test_add_visited_page will use 'db_pool' from conftest via the storage_manager fixture.
def test_add_visited_page(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    # ... (rest of the test should be okay) ...
# ... existing code ...
```

**Summary of changes for `test_storage.py` and `conftest.py`:**

*   **`conftest.py`:**
    *   The `db_pool` fixture now takes `test_config` as an argument.
    *   It creates its database file at `Path(test_config.data_dir) / "crawler_state.db"`. This ensures the pool's DB path matches what `StorageManager` expects when initialized with the same `test_config`.
*   **`test_storage.py`:**
    *   Removed its local `dummy_config_for_storage` and `temp_test_dir` fixtures.
    *   The `storage_manager` fixture now uses `test_config` (from `conftest.py`).
    *   `test_storage_manager_initialization` now uses `test_config` and its assertion `storage_manager.db_path == Path(db_pool.db_path)` should pass.
    *   `test_storage_manager_reinitialization` also uses `test_config` from conftest. It then explicitly defines `target_db_path = Path(test_config.data_dir) / "crawler_state.db"` and initializes its local pools (`pool_reinit1`, `pool_reinit2`) and `StorageManager` instances using this `target_db_path` and `test_config`. This makes the assertions about paths straightforward.

Let's apply these changes. This should fix both assertion errors related to paths.

```python:crawler/conftest.py
// ... existing code ...
@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig: # This fixture should come before db_pool if db_pool depends on it
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data" # This is the general test_data dir
    data_dir.mkdir(parents=True, exist_ok=True) 
    
    seed_file = data_dir / "seeds.txt"
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")

    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir, # This data_dir will be used by StorageManager via this config
        max_workers=1, 
        log_level="DEBUG",
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False,
    )

@pytest.fixture(scope="function")
def db_pool(test_config: CrawlerConfig) -> Iterator[SQLiteConnectionPool]: # Now depends on test_config
    """
    Pytest fixture for a SQLiteConnectionPool.
    Creates the DB inside the test_config.data_dir.
    Ensures the pool is closed after the test.
    """
    # The database file should be created based on the config's data_dir
    # This ensures StorageManager and the pool point to the same DB location.
    db_file = Path(test_config.data_dir) / "crawler_state.db" 
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True) 
    yield pool 
    pool.close_all() 

@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
// ... existing code ...
```
```python:crawler/tests/test_storage.py
import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

# Import db_pool from conftest
# pytest will automatically discover fixtures in conftest.py in the same directory or parent directories.

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig # Will use test_config from conftest
from crawler_module.db_pool import SQLiteConnectionPool # For test_storage_manager_reinitialization

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Removed local temp_test_dir and dummy_config_for_storage as we'll use conftest versions

# storage_manager fixture now uses test_config from conftest.py
@pytest.fixture
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {test_config.data_dir} and db_pool: {db_pool.db_path}")
    sm = StorageManager(config=test_config, db_pool=db_pool)
    yield sm

# Test functions will now use 'test_config' from conftest if they need config directly
def test_storage_manager_initialization(storage_manager: StorageManager, test_config: CrawlerConfig, db_pool: SQLiteConnectionPool):
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    assert Path(db_pool.db_path).exists(), f"Database file {db_pool.db_path} should exist."
    # This assertion should now pass if db_pool uses test_config.data_dir
    assert storage_manager.db_path == Path(db_pool.db_path), \
        f"SM DB path {storage_manager.db_path} should match pool DB path {Path(db_pool.db_path)}"
    
    with db_pool as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';")
        assert cursor.fetchone() is not None, "schema_version table should exist."
        cursor.close()
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test if all tables and indexes are created as expected using a connection from the pool."""
    # The storage_manager fixture already initializes the schema via StorageManager.__init__
    with db_pool as conn: # Use the pool's context manager to get a connection
        cursor = conn.cursor()

        tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
        for table in tables_to_check:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            assert cursor.fetchone() is not None, f"Table {table} should exist."

        cursor.execute("SELECT version FROM schema_version")
        assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
        assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
        assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
        
        cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager):
    """Test the URL hashing function."""
    url1 = "http://example.com/page1"
    url2 = "http://example.com/page2"
    url1_again = "http://example.com/page1"

    hash1 = storage_manager.get_url_sha256(url1)
    hash2 = storage_manager.get_url_sha256(url2)
    hash1_again = storage_manager.get_url_sha256(url1_again)

    assert isinstance(hash1, str)
    assert len(hash1) == 64 # SHA256 hex digest length
    assert hash1 != hash2
    assert hash1 == hash1_again
    logger.info("URL SHA256 hashing test passed.")

def test_storage_manager_reinitialization(test_config: CrawlerConfig, tmp_path: Path): # Uses test_config from conftest
    """Test that re-initializing StorageManager with the same path is safe."""
    
    # The standard DB path that StorageManager will use based on test_config
    target_db_path = Path(test_config.data_dir) / "crawler_state.db"

    pool_reinit1 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm1 = StorageManager(config=test_config, db_pool=pool_reinit1)
    # Verify StorageManager calculates its db_path correctly based on the config
    assert sm1.db_path == target_db_path, \
        f"SM1 path {sm1.db_path} should match target DB path {target_db_path}"
    pool_reinit1.close_all()

    pool_reinit2 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm2 = StorageManager(config=test_config, db_pool=pool_reinit2)
    assert sm2.db_path == target_db_path, \
        f"SM2 path {sm2.db_path} should match target DB path {target_db_path}"
    pool_reinit2.close_all()

    assert target_db_path.exists(), "Database file should exist after StorageManager initializations."
    logger.info("StorageManager re-initialization test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager): 
    """Test saving content to a file asynchronously."""
    url_hash = "test_url_hash_content"
    text_content = "This is some test content.\nWith multiple lines."
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is not None
    assert file_path.exists()
    assert file_path.name == f"{url_hash}.txt"
    assert file_path.parent == storage_manager.content_dir

    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
        saved_content = await f.read()
    assert saved_content == text_content
    logger.info("Save content to file test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
    """Test that saving empty content returns None and creates no file."""
    url_hash = "empty_content_hash"
    text_content = ""
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    # Ensure no file was created
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save empty content to file test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
    """Test handling of IOError during file save."""
    url_hash = "io_error_hash"
    text_content = "Some content that will fail to save."

    # Mock aiofiles.open to raise IOError
    async def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Simulated disk full error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save content to file with IOError test passed.")

def test_add_visited_page(storage_manager: StorageManager, db_pool: SQLiteConnectionPool): 
    """Test adding a visited page record to the database."""
    url = "http://example.com/visited_page"
    domain = "example.com"
    status_code = 200
    crawled_timestamp = int(time.time())
    content_type = "text/html"
    text_content = "<html><body>Visited!</body></html>"
    url_hash_for_file = storage_manager.get_url_sha256(url)
    content_storage_path_str = str(storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_text=text_content, 
        content_storage_path_str=content_storage_path_str,
        redirected_to_url=None
    )

    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

        cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    (db_url_sha256, db_url, db_domain, db_crawled_ts, db_status, 
     db_content_type, db_content_hash, db_storage_path, db_redirected_to) = row
    
    assert db_url_sha256 == expected_url_sha256
    assert db_url == url
    assert db_domain == domain
    assert db_crawled_ts == crawled_timestamp
    assert db_status == status_code
    assert db_content_type == content_type
    assert db_content_hash == expected_content_hash
    assert db_storage_path == content_storage_path_str
    assert db_redirected_to is None
    logger.info("Add visited page test passed.")

def test_add_visited_page_no_content(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
    url = "http://example.com/redirect"
    domain = "example.com"
    status_code = 301
    crawled_timestamp = int(time.time())
    redirected_to_url = "http://example.com/final_destination"

    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=None,
        content_text=None,
        content_storage_path_str=None,
        redirected_to_url=redirected_to_url
    )

    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    content_hash, content_storage_path, db_redirected_to = row
    assert content_hash is None
    assert content_storage_path is None
    assert db_redirected_to == redirected_to_url
    logger.info("Add visited page with no content test passed.")
```

```python
// ... existing code ...
@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig: # This fixture should come before db_pool if db_pool depends on it
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data" # This is the general test_data dir
    data_dir.mkdir(parents=True, exist_ok=True) 
    
    seed_file = data_dir / "seeds.txt"
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")

    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir, # This data_dir will be used by StorageManager via this config
        max_workers=1, 
        log_level="DEBUG",
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False,
    )

@pytest.fixture(scope="function")
def db_pool(test_config: CrawlerConfig) -> Iterator[SQLiteConnectionPool]: # Now depends on test_config
    """
    Pytest fixture for a SQLiteConnectionPool.
    Creates the DB inside the test_config.data_dir.
    Ensures the pool is closed after the test.
    """
    # The database file should be created based on the config's data_dir
    # This ensures StorageManager and the pool point to the same DB location.
    db_file = Path(test_config.data_dir) / "crawler_state.db" 
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True) 
    yield pool 
    pool.close_all() 

@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
// ... existing code ...
```

```python
import pytest
from pathlib import Path
import sqlite3
// ... other imports ...
from crawler_module.config import CrawlerConfig # Will use test_config from conftest
from crawler_module.db_pool import SQLiteConnectionPool

# ... logger setup ...

# Remove temp_test_dir and dummy_config_for_storage if using conftest versions
# @pytest.fixture
# def temp_test_dir(tmp_path: Path) -> Path: ...
# @pytest.fixture
# def dummy_config_for_storage(temp_test_dir: Path) -> CrawlerConfig: ...

# storage_manager fixture now uses test_config from conftest.py
@pytest.fixture
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {test_config.data_dir} and db_pool: {db_pool.db_path}")
    sm = StorageManager(config=test_config, db_pool=db_pool)
    yield sm

# Test functions will now use 'test_config' if they need config directly
def test_storage_manager_initialization(storage_manager: StorageManager, test_config: CrawlerConfig, db_pool: SQLiteConnectionPool):
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    assert Path(db_pool.db_path).exists(), f"Database file {db_pool.db_path} should exist."
    # This assertion should now pass if db_pool uses test_config.data_dir
    assert storage_manager.db_path == Path(db_pool.db_path), \
        f"SM DB path {storage_manager.db_path} should match pool DB path {Path(db_pool.db_path)}"
    
    with db_pool as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';")
        assert cursor.fetchone() is not None, "schema_version table should exist."
        cursor.close()
    logger.info("StorageManager initialization test passed.")

# test_database_schema_creation should be fine as it uses storage_manager and db_pool

# test_get_url_sha256 is fine

def test_storage_manager_reinitialization(test_config: CrawlerConfig, tmp_path: Path): # Use test_config from conftest
    """Test that re-initializing StorageManager with the same path is safe."""
    
    # For this test, we want to ensure re-initialization on the *exact same DB file*.
    # The test_config.data_dir is from tmp_path/test_crawler_data.
    # We'll create a specific DB file within that for this test.
    db_file_for_this_test = Path(test_config.data_dir) / "reinit_specific_for_storage.db"

    # Create a config that will definitely use this data_dir for StorageManager's db_path calculation
    # The test_config passed in already has the correct data_dir.

    pool1 = SQLiteConnectionPool(db_path=db_file_for_this_test, pool_size=1)
    # StorageManager will calculate its db_path as test_config.data_dir / "crawler_state.db"
    # The pool is explicitly told to use db_file_for_this_test. These must align.
    # To make them align, StorageManager must be initialized with a config
    # such that config.data_dir / "crawler_state.db" == db_file_for_this_test
    # OR, the pool must use config.data_dir / "crawler_state.db".
    # Let's make the pool use the standard DB name within the config's data_dir.
    
    # For reinitialization test, we explicitly control the DB file.
    # The StorageManager will derive its expected DB path from its config.
    # The pool will use the path it's given.
    # The critical thing is that StorageManager's *operations* go to the pool's DB.
    
    # Let's make config for SM point to a dir, and the pool use a DB within that same dir.
    # test_config.data_dir is fine.
    # The standard DB name used by StorageManager is "crawler_state.db".
    # So, the pool for this test should also use that name within test_config.data_dir.
    
    target_db_path = Path(test_config.data_dir) / "crawler_state.db" # Standard path

    pool_reinit1 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm1 = StorageManager(config=test_config, db_pool=pool_reinit1)
    assert sm1.db_path == target_db_path, f"SM1 path {sm1.db_path} vs target {target_db_path}"
    pool_reinit1.close_all()

    pool_reinit2 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm2 = StorageManager(config=test_config, db_pool=pool_reinit2)
    assert sm2.db_path == target_db_path, f"SM2 path {sm2.db_path} vs target {target_db_path}"
    pool_reinit2.close_all()

    assert target_db_path.exists(), "Database file should exist after StorageManager initializations."
    logger.info("StorageManager re-initialization test passed.")

# Remaining tests use storage_manager fixture, which should now be correctly configured.
# test_add_visited_page will use 'db_pool' from conftest via the storage_manager fixture.
def test_add_visited_page(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    # ... (rest of the test should be okay) ...
# ... existing code ...
```

```python
// ... existing code ...
@pytest.fixture(scope="function")
def test_config(tmp_path: Path) -> CrawlerConfig: # This fixture should come before db_pool if db_pool depends on it
    """
    Pytest fixture for a basic CrawlerConfig for testing.
    Uses a temporary data directory.
    """
    data_dir = tmp_path / "test_crawler_data" # This is the general test_data dir
    data_dir.mkdir(parents=True, exist_ok=True) 
    
    seed_file = data_dir / "seeds.txt"
    if not seed_file.exists():
        seed_file.write_text("http://example.com/default_seed\n")

    return CrawlerConfig(
        seed_file=seed_file,
        data_dir=data_dir, # This data_dir will be used by StorageManager via this config
        max_workers=1, 
        log_level="DEBUG",
        user_agent="TestCrawler/1.0",
        max_pages=10,
        max_duration=60,
        resume=False,
    )

@pytest.fixture(scope="function")
def db_pool(test_config: CrawlerConfig) -> Iterator[SQLiteConnectionPool]: # Now depends on test_config
    """
    Pytest fixture for a SQLiteConnectionPool.
    Creates the DB inside the test_config.data_dir.
    Ensures the pool is closed after the test.
    """
    # The database file should be created based on the config's data_dir
    # This ensures StorageManager and the pool point to the same DB location.
    db_file = Path(test_config.data_dir) / "crawler_state.db" 
    pool = SQLiteConnectionPool(db_path=db_file, pool_size=1, wal_mode=True) 
    yield pool 
    pool.close_all() 

@pytest.fixture(scope="function")
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
// ... existing code ...
```

```python
import pytest
from pathlib import Path
import sqlite3
import shutil
import logging
from dataclasses import dataclass
import aiofiles # For testing async file save
import time
import hashlib

# Import db_pool from conftest
# pytest will automatically discover fixtures in conftest.py in the same directory or parent directories.

from crawler_module.storage import StorageManager, DB_SCHEMA_VERSION
from crawler_module.config import CrawlerConfig # Will use test_config from conftest
from crawler_module.db_pool import SQLiteConnectionPool # For test_storage_manager_reinitialization

# Configure basic logging for tests to see output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Removed local temp_test_dir and dummy_config_for_storage as we'll use conftest versions

# storage_manager fixture now uses test_config from conftest.py
@pytest.fixture
def storage_manager(test_config: CrawlerConfig, db_pool: SQLiteConnectionPool) -> StorageManager:
    """Fixture to create a StorageManager instance using a db_pool."""
    logger.debug(f"Initializing StorageManager with config data_dir: {test_config.data_dir} and db_pool: {db_pool.db_path}")
    sm = StorageManager(config=test_config, db_pool=db_pool)
    yield sm

# Test functions will now use 'test_config' from conftest if they need config directly
def test_storage_manager_initialization(storage_manager: StorageManager, test_config: CrawlerConfig, db_pool: SQLiteConnectionPool):
    """Test if StorageManager initializes directories and DB correctly."""
    assert storage_manager.data_dir.exists()
    assert storage_manager.content_dir.exists()
    assert Path(db_pool.db_path).exists(), f"Database file {db_pool.db_path} should exist."
    # This assertion should now pass if db_pool uses test_config.data_dir
    assert storage_manager.db_path == Path(db_pool.db_path), \
        f"SM DB path {storage_manager.db_path} should match pool DB path {Path(db_pool.db_path)}"
    
    with db_pool as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';")
        assert cursor.fetchone() is not None, "schema_version table should exist."
        cursor.close()
    logger.info("StorageManager initialization test passed.")

def test_database_schema_creation(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test if all tables and indexes are created as expected using a connection from the pool."""
    # The storage_manager fixture already initializes the schema via StorageManager.__init__
    with db_pool as conn: # Use the pool's context manager to get a connection
        cursor = conn.cursor()

        tables_to_check = ["frontier", "visited_urls", "domain_metadata", "schema_version"]
        for table in tables_to_check:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
            assert cursor.fetchone() is not None, f"Table {table} should exist."

        cursor.execute("SELECT version FROM schema_version")
        assert cursor.fetchone()[0] == DB_SCHEMA_VERSION, f"Schema version should be {DB_SCHEMA_VERSION}"

        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_frontier_domain';")
        assert cursor.fetchone() is not None, "Index idx_frontier_domain on frontier table should exist."
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_visited_domain';")
        assert cursor.fetchone() is not None, "Index idx_visited_domain on visited_urls table should exist."
        
        cursor.close()
    logger.info("Database schema creation test passed.")

def test_get_url_sha256(storage_manager: StorageManager):
    """Test the URL hashing function."""
    url1 = "http://example.com/page1"
    url2 = "http://example.com/page2"
    url1_again = "http://example.com/page1"

    hash1 = storage_manager.get_url_sha256(url1)
    hash2 = storage_manager.get_url_sha256(url2)
    hash1_again = storage_manager.get_url_sha256(url1_again)

    assert isinstance(hash1, str)
    assert len(hash1) == 64 # SHA256 hex digest length
    assert hash1 != hash2
    assert hash1 == hash1_again
    logger.info("URL SHA256 hashing test passed.")

def test_storage_manager_reinitialization(test_config: CrawlerConfig, tmp_path: Path): # Uses test_config from conftest
    """Test that re-initializing StorageManager with the same path is safe."""
    
    # The standard DB path that StorageManager will use based on test_config
    target_db_path = Path(test_config.data_dir) / "crawler_state.db"

    pool_reinit1 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm1 = StorageManager(config=test_config, db_pool=pool_reinit1)
    # Verify StorageManager calculates its db_path correctly based on the config
    assert sm1.db_path == target_db_path, \
        f"SM1 path {sm1.db_path} should match target DB path {target_db_path}"
    pool_reinit1.close_all()

    pool_reinit2 = SQLiteConnectionPool(db_path=target_db_path, pool_size=1)
    sm2 = StorageManager(config=test_config, db_pool=pool_reinit2)
    assert sm2.db_path == target_db_path, \
        f"SM2 path {sm2.db_path} should match target DB path {target_db_path}"
    pool_reinit2.close_all()

    assert target_db_path.exists(), "Database file should exist after StorageManager initializations."
    logger.info("StorageManager re-initialization test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file(storage_manager: StorageManager): 
    """Test saving content to a file asynchronously."""
    url_hash = "test_url_hash_content"
    text_content = "This is some test content.\nWith multiple lines."
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is not None
    assert file_path.exists()
    assert file_path.name == f"{url_hash}.txt"
    assert file_path.parent == storage_manager.content_dir

    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
        saved_content = await f.read()
    assert saved_content == text_content
    logger.info("Save content to file test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file_empty_content(storage_manager: StorageManager):
    """Test that saving empty content returns None and creates no file."""
    url_hash = "empty_content_hash"
    text_content = ""
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    # Ensure no file was created
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save empty content to file test passed.")

@pytest.mark.asyncio
async def test_save_content_to_file_io_error(storage_manager: StorageManager, monkeypatch):
    """Test handling of IOError during file save."""
    url_hash = "io_error_hash"
    text_content = "Some content that will fail to save."

    # Mock aiofiles.open to raise IOError
    async def mock_aio_open_raiser(*args, **kwargs):
        raise IOError("Simulated disk full error")

    monkeypatch.setattr(aiofiles, "open", mock_aio_open_raiser)
    
    file_path = await storage_manager.save_content_to_file(url_hash, text_content)
    
    assert file_path is None
    expected_file = storage_manager.content_dir / f"{url_hash}.txt"
    assert not expected_file.exists()
    logger.info("Save content to file with IOError test passed.")

def test_add_visited_page(storage_manager: StorageManager, db_pool: SQLiteConnectionPool): 
    """Test adding a visited page record to the database."""
    url = "http://example.com/visited_page"
    domain = "example.com"
    status_code = 200
    crawled_timestamp = int(time.time())
    content_type = "text/html"
    text_content = "<html><body>Visited!</body></html>"
    url_hash_for_file = storage_manager.get_url_sha256(url)
    content_storage_path_str = str(storage_manager.content_dir / f"{url_hash_for_file}.txt")
    
    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=content_type,
        content_text=text_content, 
        content_storage_path_str=content_storage_path_str,
        redirected_to_url=None
    )

    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        expected_content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

        cursor.execute("SELECT * FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    (db_url_sha256, db_url, db_domain, db_crawled_ts, db_status, 
     db_content_type, db_content_hash, db_storage_path, db_redirected_to) = row
    
    assert db_url_sha256 == expected_url_sha256
    assert db_url == url
    assert db_domain == domain
    assert db_crawled_ts == crawled_timestamp
    assert db_status == status_code
    assert db_content_type == content_type
    assert db_content_hash == expected_content_hash
    assert db_storage_path == content_storage_path_str
    assert db_redirected_to is None
    logger.info("Add visited page test passed.")

def test_add_visited_page_no_content(storage_manager: StorageManager, db_pool: SQLiteConnectionPool):
    """Test adding a visited page with no text content (e.g., redirect or non-HTML)."""
    url = "http://example.com/redirect"
    domain = "example.com"
    status_code = 301
    crawled_timestamp = int(time.time())
    redirected_to_url = "http://example.com/final_destination"

    storage_manager.add_visited_page(
        url=url,
        domain=domain,
        status_code=status_code,
        crawled_timestamp=crawled_timestamp,
        content_type=None,
        content_text=None,
        content_storage_path_str=None,
        redirected_to_url=redirected_to_url
    )

    with db_pool as conn:
        cursor = conn.cursor()
        expected_url_sha256 = storage_manager.get_url_sha256(url)
        cursor.execute("SELECT content_hash, content_storage_path, redirected_to_url FROM visited_urls WHERE url_sha256 = ?", (expected_url_sha256,))
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    content_hash, content_storage_path, db_redirected_to = row
    assert content_hash is None
    assert content_storage_path is None
    assert db_redirected_to == redirected_to_url
    logger.info("Add visited page with no content test passed.")
```

---

**User**

great job! all tests passing

---

**Cursor**

Excellent! That's great news that all tests in `test_storage.py` are passing now.
The key was ensuring that the `db_pool` fixture in `conftest.py` created its database file in a location consistent with the `data_dir` of the `test_config` it was used with. This made `storage_manager.db_path` (derived from its config) and `db_pool.db_path` (where the pool actually creates the DB) align correctly.

The linter errors about missing library stubs (e.g., for `aiofiles`) and not finding module implementations (`crawler_module.*`) are separate issues, likely related to the Python environment setup for linting/type-checking rather than the test logic itself. We can address those later if needed.

**Next Steps:**

Now that `test_storage.py` is green, we should revisit the other test files that were failing:

1.  **`tests/test_frontier.py`**:
    *   `FAILED tests/test_frontier.py::test_frontier_resume_with_politeness - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'`
    *   `ERROR tests/test_frontier.py::test_frontier_initialization_new - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'`
    *   `ERROR tests/test_frontier.py::test_add_and_get_urls - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'`
    We had proposed changes for this file. I can re-apply or generate them now based on our current understanding of `conftest.py`. The main idea will be to ensure that `StorageManager` instances created within this test file (either directly or via fixtures like `storage_manager_for_frontier`) are correctly provided with a `db_pool`.

2.  **`tests/test_politeness.py`**:
    *   `ERROR tests/test_politeness.py::test_load_manual_exclusions_with_file - TypeError: StorageManager.__init__() missing 1 required positional argument: 'db_pool'`
    *   And the other tests in this file that need their mocking strategy updated due to `PolitenessEnforcer` now using the pool.
    We've already started refactoring this file. I applied changes to `test_is_url_allowed_by_robots` and `test_load_manual_exclusions_with_file`. We need to continue updating the remaining tests.

Which file would you like to tackle next? `tests/test_frontier.py` or continue with `tests/test_politeness.py`?

---

**User**

they are now actually all passing as well, you might have been reading an outdated terminal output

---

**Cursor**

That's fantastic news! My apologies, I must have been looking at an older cached output of the test results.

If all tests in `test_frontier.py` and `test_politeness.py` are also passing now, it means our refactoring efforts to integrate the `SQLiteConnectionPool` and update the test fixtures have been successful across the board. This is a major step forward!

This strongly suggests that the primary cause of the "too many open files" error (Problem 1) – which was the constant opening and closing of SQLite connections – should now be resolved. The connection pool will manage a fixed set of connections, significantly reducing the churn of file descriptors for the database.

**Next Steps:**

1.  **Verify Scalability (Problem 1 Resolution):**
    *   The most important next step is to test the crawler with an increased number of workers (e.g., try `max_workers = 50` or `max_workers = 100` in your configuration).
    *   Monitor the crawler for any signs of the "too many open files" error. You can also keep an eye on the number of open file descriptors for the crawler process if you have monitoring in place (`psutil.Process(os.getpid()).num_fds()`).
    *   If it runs stably with a higher worker count, we can consider Problem 1 addressed.

2.  **Diagnose Long-Run Instability (Problem 2):**
    *   Once we're confident about scaling workers, we can shift our full attention to the issue where network utilization drops over time and the program gets terminated after 12+ hours.
    *   As discussed before, for this, we'll need:
        *   **Crawler logs** from such a long run, especially towards the end or any sections with errors or unusual behavior.
        *   **System logs** around the time of termination (e.g., from `dmesg`, `journalctl`, or `/var/log/syslog`) to check for OOM killer messages or other OS-level reasons for termination.

This is excellent progress! Let me know how the high-worker-count tests go, and then we can dive into the logs for Problem 2.

