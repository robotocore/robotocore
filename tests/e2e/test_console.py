"""End-to-end Playwright tests for the AWS Console web UI.

These tests require the robotocore server to be running on port 4566.
They use a headless Chromium browser to interact with the console.
"""

import pytest
from playwright.sync_api import Page, expect

BASE_URL = "http://localhost:4566/_robotocore/console"


@pytest.fixture(autouse=True)
def _navigate_to_console(page: Page):
    """Navigate to the console before each test."""
    page.goto(BASE_URL)
    page.wait_for_load_state("networkidle")


class TestDashboard:
    """Tests for the Dashboard / Overview page."""

    def test_dashboard_loads(self, page: Page):
        """Console loads and shows the dashboard by default."""
        expect(page.locator("#page-title")).to_contain_text("Dashboard")

    def test_sidebar_visible(self, page: Page):
        """Sidebar navigation is visible with service links."""
        sidebar = page.locator("#sidebar")
        expect(sidebar).to_be_visible()
        expect(sidebar.locator("a[data-page='s3']")).to_be_visible()
        expect(sidebar.locator("a[data-page='dynamodb']")).to_be_visible()
        expect(sidebar.locator("a[data-page='sqs']")).to_be_visible()
        expect(sidebar.locator("a[data-page='lambda']")).to_be_visible()
        expect(sidebar.locator("a[data-page='cloudwatch']")).to_be_visible()

    def test_header_shows_region(self, page: Page):
        """Header displays region selector."""
        region_select = page.locator("#region-select")
        expect(region_select).to_be_visible()

    def test_stat_cards_present(self, page: Page):
        """Dashboard shows stat cards after loading."""
        page.wait_for_selector(".stat-card", timeout=10000)
        cards = page.locator(".stat-card")
        expect(cards.first).to_be_visible()

    def test_service_cards_present(self, page: Page):
        """Dashboard shows service cards."""
        page.wait_for_selector(".service-card", timeout=10000)
        cards = page.locator(".service-card")
        expect(cards.first).to_be_visible()


class TestS3Console:
    """Tests for the S3 console page."""

    def test_navigate_to_s3(self, page: Page):
        """Clicking S3 in sidebar navigates to S3 page."""
        page.locator("#sidebar a[data-page='s3']").click()
        page.wait_for_load_state("networkidle")
        expect(page.locator("#page-title")).to_contain_text("S3 Buckets")

    def test_s3_shows_bucket_list(self, page: Page):
        """S3 page shows bucket list (may be empty)."""
        page.goto(f"{BASE_URL}#s3")
        page.wait_for_load_state("networkidle")
        page.wait_for_selector(".section-header", timeout=10000)
        header = page.locator(".section-header h2")
        expect(header).to_contain_text("Buckets")

    def test_s3_create_bucket_flow(self, page: Page):
        """S3 create bucket page renders correctly."""
        page.goto(f"{BASE_URL}#s3/create")
        page.wait_for_load_state("networkidle")
        expect(page.locator("#page-title")).to_contain_text("Create Bucket")
        expect(page.locator("#create-bucket-name")).to_be_visible()
        expect(page.locator("#create-bucket-submit")).to_be_visible()

    def test_s3_create_and_delete_bucket(self, page: Page):
        """Create a bucket, verify it appears, then delete it."""
        # Navigate to create bucket
        page.goto(f"{BASE_URL}#s3/create")
        page.wait_for_load_state("networkidle")
        page.wait_for_selector("#create-bucket-name", timeout=10000)

        # Fill in bucket name and create
        bucket_name = "e2e-test-bucket-console"
        page.fill("#create-bucket-name", bucket_name)
        page.click("#create-bucket-submit")

        # Wait for navigation back to bucket list and data to load
        page.wait_for_timeout(3000)
        page.wait_for_load_state("networkidle")
        page.wait_for_selector(".section-header", timeout=10000)

        # Verify bucket appears in the table
        expect(page.locator(f"td strong:has-text('{bucket_name}')")).to_be_visible()

        # Delete the bucket
        row = page.locator(f"tr:has-text('{bucket_name}')")
        page.on("dialog", lambda dialog: dialog.accept())
        row.locator(".s3-delete-btn").click()

        # Wait for deletion and page refresh
        page.wait_for_timeout(3000)


class TestDynamoDBConsole:
    """Tests for the DynamoDB console page."""

    def test_navigate_to_dynamodb(self, page: Page):
        """Clicking DynamoDB in sidebar navigates to DynamoDB page."""
        page.locator("#sidebar a[data-page='dynamodb']").click()
        page.wait_for_load_state("networkidle")
        expect(page.locator("#page-title")).to_contain_text("DynamoDB Tables")

    def test_dynamodb_shows_table_list(self, page: Page):
        """DynamoDB page shows table list."""
        page.goto(f"{BASE_URL}#dynamodb")
        page.wait_for_load_state("networkidle")
        page.wait_for_selector(".section-header", timeout=10000)
        header = page.locator(".section-header h2")
        expect(header).to_contain_text("Tables")

    def test_dynamodb_create_table_form(self, page: Page):
        """DynamoDB create table page renders correctly."""
        page.goto(f"{BASE_URL}#dynamodb/create")
        page.wait_for_load_state("networkidle")
        expect(page.locator("#page-title")).to_contain_text("Create Table")
        expect(page.locator("#ct-name")).to_be_visible()
        expect(page.locator("#ct-pk")).to_be_visible()
        expect(page.locator("#ct-submit")).to_be_visible()


class TestSQSConsole:
    """Tests for the SQS console page."""

    def test_navigate_to_sqs(self, page: Page):
        """Clicking SQS in sidebar navigates to SQS page."""
        page.locator("#sidebar a[data-page='sqs']").click()
        page.wait_for_load_state("networkidle")
        expect(page.locator("#page-title")).to_contain_text("SQS Queues")

    def test_sqs_shows_queue_list(self, page: Page):
        """SQS page shows queue list."""
        page.goto(f"{BASE_URL}#sqs")
        page.wait_for_load_state("networkidle")
        page.wait_for_selector(".section-header", timeout=10000)
        header = page.locator(".section-header h2")
        expect(header).to_contain_text("Queues")

    def test_sqs_create_queue_form(self, page: Page):
        """SQS create queue page renders correctly."""
        page.goto(f"{BASE_URL}#sqs/create")
        page.wait_for_load_state("networkidle")
        expect(page.locator("#page-title")).to_contain_text("Create Queue")
        expect(page.locator("#cq-name")).to_be_visible()
        expect(page.locator("#cq-submit")).to_be_visible()


class TestLambdaConsole:
    """Tests for the Lambda console page."""

    def test_navigate_to_lambda(self, page: Page):
        """Clicking Lambda in sidebar navigates to Lambda page."""
        page.locator("#sidebar a[data-page='lambda']").click()
        page.wait_for_load_state("networkidle")
        expect(page.locator("#page-title")).to_contain_text("Lambda Functions")

    def test_lambda_shows_function_list(self, page: Page):
        """Lambda page shows function list."""
        page.goto(f"{BASE_URL}#lambda")
        page.wait_for_load_state("networkidle")
        page.wait_for_selector(".section-header", timeout=10000)
        header = page.locator(".section-header h2")
        expect(header).to_contain_text("Functions")


class TestCloudWatchConsole:
    """Tests for the CloudWatch Logs console page."""

    def test_navigate_to_cloudwatch(self, page: Page):
        """Clicking CloudWatch in sidebar navigates to CloudWatch page."""
        page.locator("#sidebar a[data-page='cloudwatch']").click()
        page.wait_for_load_state("networkidle")
        expect(page.locator("#page-title")).to_contain_text("CloudWatch Log Groups")

    def test_cloudwatch_shows_log_groups(self, page: Page):
        """CloudWatch page shows log groups list."""
        page.goto(f"{BASE_URL}#cloudwatch")
        page.wait_for_load_state("networkidle")
        page.wait_for_selector(".section-header", timeout=10000)
        header = page.locator(".section-header h2")
        expect(header).to_contain_text("Log Groups")


class TestNavigation:
    """Tests for SPA navigation between pages."""

    def test_hash_navigation(self, page: Page):
        """Hash-based navigation works for all pages."""
        pages_and_titles = [
            ("#s3", "S3 Buckets"),
            ("#dynamodb", "DynamoDB Tables"),
            ("#sqs", "SQS Queues"),
            ("#lambda", "Lambda Functions"),
            ("#cloudwatch", "CloudWatch Log Groups"),
            ("#dashboard", "Dashboard"),
        ]
        for hash_val, expected_title in pages_and_titles:
            page.goto(f"{BASE_URL}{hash_val}")
            page.wait_for_load_state("networkidle")
            page.wait_for_selector("#page-title", timeout=10000)
            title = page.locator("#page-title")
            expect(title).to_contain_text(expected_title)

    def test_breadcrumb_updates(self, page: Page):
        """Breadcrumb updates when navigating between pages."""
        page.goto(f"{BASE_URL}#s3")
        page.wait_for_load_state("networkidle")
        breadcrumb = page.locator("#breadcrumb")
        expect(breadcrumb).to_contain_text("S3")

        page.goto(f"{BASE_URL}#dynamodb")
        page.wait_for_load_state("networkidle")
        expect(breadcrumb).to_contain_text("DynamoDB")

    def test_sidebar_active_state(self, page: Page):
        """Sidebar highlights the active page."""
        page.goto(f"{BASE_URL}#s3")
        page.wait_for_load_state("networkidle")
        active_link = page.locator(".nav-item.active")
        expect(active_link).to_contain_text("S3")


class TestStaticAssets:
    """Tests for static file serving."""

    def test_css_loads(self, page: Page):
        """CSS file loads successfully."""
        response = page.request.get(f"{BASE_URL}/static/style.css")
        assert response.status == 200
        assert "text/css" in response.headers.get("content-type", "")

    def test_js_loads(self, page: Page):
        """Main JS file loads successfully."""
        response = page.request.get(f"{BASE_URL}/static/app.js")
        assert response.status == 200
        assert "javascript" in response.headers.get("content-type", "")

    def test_service_js_loads(self, page: Page):
        """Service JS files load successfully."""
        services = ["s3", "dynamodb", "sqs", "lambda", "cloudwatch"]
        for svc in services:
            response = page.request.get(f"{BASE_URL}/static/services/{svc}.js")
            assert response.status == 200, f"Failed to load {svc}.js"
            assert "javascript" in response.headers.get("content-type", "")

    def test_directory_traversal_blocked(self, page: Page):
        """Directory traversal attempts are blocked."""
        response = page.request.get(f"{BASE_URL}/static/../../../etc/passwd")
        assert response.status in (400, 403, 404)


class TestAPIProxy:
    """Tests for the console API proxy endpoint."""

    def test_api_proxy_s3_list_buckets(self, page: Page):
        """API proxy can list S3 buckets."""
        response = page.request.post(
            f"{BASE_URL}/api/s3/ListBuckets",
            data="{}",
            headers={"Content-Type": "application/json"},
        )
        assert response.status == 200

    def test_api_proxy_dynamodb_list_tables(self, page: Page):
        """API proxy can list DynamoDB tables."""
        response = page.request.post(
            f"{BASE_URL}/api/dynamodb/ListTables",
            data="{}",
            headers={"Content-Type": "application/json"},
        )
        assert response.status == 200
        body = response.json()
        assert "TableNames" in body

    def test_api_proxy_lambda_list_functions(self, page: Page):
        """API proxy can list Lambda functions."""
        response = page.request.post(
            f"{BASE_URL}/api/lambda/ListFunctions",
            data="{}",
            headers={"Content-Type": "application/json"},
        )
        assert response.status == 200

    def test_api_proxy_missing_service(self, page: Page):
        """API proxy returns 400 for missing service."""
        response = page.request.post(
            f"{BASE_URL}/api//ListBuckets",
            data="{}",
            headers={"Content-Type": "application/json"},
        )
        # Empty service should fail
        assert response.status in (400, 404)
