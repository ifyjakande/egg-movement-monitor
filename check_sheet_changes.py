#!/usr/bin/env python3
"""
Google Sheets Change Detection with Content Hashing
Monitors multiple worksheets and sends Google Chat Card alerts on changes
"""

import json
import os
import sys
import hashlib
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import gspread
import requests


def get_credentials():
    """Get Google API credentials from environment variable."""
    service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT')
    if not service_account_json:
        print("ERROR: GOOGLE_SERVICE_ACCOUNT environment variable not set")
        sys.exit(1)

    try:
        credentials_dict = json.loads(service_account_json)
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.metadata.readonly'
            ]
        )
        print("Authentication successful")
        return credentials

    except json.JSONDecodeError:
        print("ERROR: Invalid service account JSON format")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Authentication failed - {e}")
        sys.exit(1)


def get_worksheet_hash(spreadsheet_id, worksheet_name, credentials):
    """Get content hash of a specific worksheet."""
    try:
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Get all values from the worksheet
        all_values = worksheet.get_all_values()

        # Create hash of the content
        content_str = str(all_values)
        content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()

        return content_hash

    except gspread.WorksheetNotFound:
        print(f"ERROR: Worksheet not found")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to fetch worksheet data - {e}")
        sys.exit(1)


def load_last_hashes():
    """Load the last processed content hashes from file."""
    hash_file = 'last_source_hash.json'
    try:
        if os.path.exists(hash_file):
            with open(hash_file, 'r') as f:
                data = json.load(f)
                return data
        return {}
    except Exception as e:
        print(f"WARNING: Could not load previous hashes - {e}")
        return {}


def save_hashes(hashes_data):
    """Save the current content hashes to file."""
    hash_file = 'last_source_hash.json'
    try:
        with open(hash_file, 'w') as f:
            json.dump(hashes_data, f, indent=2)
        print("Hash file updated successfully")
    except Exception as e:
        print(f"ERROR: Failed to save hashes - {e}")
        sys.exit(1)


def send_google_chat_card(webhook_url, changed_worksheets, spreadsheet_id):
    """Send a rich card notification to Google Chat."""
    try:
        # Create timestamp in WAT (UTC+1)
        wat_time = datetime.utcnow() + timedelta(hours=1)
        timestamp = wat_time.strftime('%Y-%m-%d %H:%M:%S WAT')

        # Build the changed worksheets text
        worksheets_text = "\n".join([f"• {ws_name}" for ws_name in changed_worksheets])

        # Build Google Chat Card
        card = {
            "cards": [
                {
                    "header": {
                        "title": "🔔 Egg Movement Tracker - Changes Detected",
                        "subtitle": "Source data has been updated",
                        "imageUrl": "https://www.gstatic.com/images/branding/product/1x/sheets_48dp.png"
                    },
                    "sections": [
                        {
                            "widgets": [
                                {
                                    "keyValue": {
                                        "topLabel": "Changed Worksheets",
                                        "content": f"{len(changed_worksheets)} worksheet(s) updated",
                                        "icon": "DESCRIPTION"
                                    }
                                },
                                {
                                    "textParagraph": {
                                        "text": worksheets_text
                                    }
                                }
                            ]
                        },
                        {
                            "widgets": [
                                {
                                    "keyValue": {
                                        "topLabel": "Detection Time",
                                        "content": timestamp,
                                        "icon": "CLOCK"
                                    }
                                },
                                {
                                    "buttons": [
                                        {
                                            "textButton": {
                                                "text": "VIEW SHEET",
                                                "onClick": {
                                                    "openLink": {
                                                        "url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        response = requests.post(webhook_url, json=card, timeout=10)

        if response.status_code == 200:
            print("Alert sent successfully")
            return True
        else:
            print(f"WARNING: Alert delivery failed with status {response.status_code}")
            return False

    except Exception as e:
        print(f"ERROR: Failed to send alert - {e}")
        return False


def main():
    """Main function to check for changes in source worksheets."""
    try:
        print("Starting change detection...")

        # Get environment variables
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        source_worksheets = os.getenv('SOURCE_WORKSHEETS')
        webhook_url = os.getenv('GOOGLE_CHAT_WEBHOOK')

        # Validate environment variables
        if not spreadsheet_id:
            print("ERROR: SPREADSHEET_ID not set")
            sys.exit(1)

        if not source_worksheets:
            print("ERROR: SOURCE_WORKSHEETS not set")
            sys.exit(1)

        if not webhook_url:
            print("ERROR: GOOGLE_CHAT_WEBHOOK not set")
            sys.exit(1)

        # Parse worksheet names
        worksheet_names = [name.strip() for name in source_worksheets.split(',')]
        print(f"Monitoring {len(worksheet_names)} worksheet(s)")

        # Get credentials
        credentials = get_credentials()

        # Load previous hashes
        last_hashes = load_last_hashes()

        # Check each worksheet for changes
        current_hashes = {}
        changed_worksheets = []

        for ws_name in worksheet_names:
            current_hash = get_worksheet_hash(spreadsheet_id, ws_name, credentials)
            current_hashes[ws_name] = current_hash

            # Compare with previous hash
            last_hash = last_hashes.get(ws_name)
            if last_hash != current_hash:
                changed_worksheets.append(ws_name)

        # Add timestamp
        current_hashes['last_checked'] = datetime.utcnow().isoformat()

        # Determine if update is needed
        if changed_worksheets:
            print(f"Changes detected in {len(changed_worksheets)} worksheet(s)")

            # Send Google Chat alert
            send_google_chat_card(webhook_url, changed_worksheets, spreadsheet_id)

            # Save new hashes
            save_hashes(current_hashes)

            return True
        else:
            print("No changes detected")

            # Update timestamp even if no changes
            save_hashes(current_hashes)

            return False

    except Exception as e:
        print(f"ERROR: Unexpected failure - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
