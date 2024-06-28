from __future__ import unicode_literals
import frappe
from frappe import throw, msgprint, _
from datetime import date
import json
import requests
import time

@frappe.whitelist()
def add_source_lead():
    if not frappe.db.exists("Lead Source", "India Mart"):
        if not frappe.has_permission("Lead Source", "create"):
            frappe.throw(_("Not permitted to create Lead Source"), frappe.PermissionError)
        
        doc = frappe.get_doc({
            "doctype": "Lead Source",
            "source_name": "India Mart"
        })
        doc.insert()
        frappe.msgprint(_("Lead Source Added For India Mart"))
    else:
        frappe.msgprint(_("India Mart Lead Source Already Available"))

@frappe.whitelist()
def sync_india_mart_lead(from_date, to_date):
    try:
        india_mart_setting = frappe.get_doc("IndiaMart Setting", "IndiaMart Setting")
        if not (india_mart_setting.url and india_mart_setting.mobile_no and india_mart_setting.key):
            frappe.throw(
                msg=_('URL, Mobile, Key mandatory for Indiamart API Call. Please set them and try again.'),
                title=_('Missing Setting Fields')
            )
        req = get_request_url(india_mart_setting, from_date, to_date)
        
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            res = requests.post(url=req)
            
            if res.status_code == 429:
                if attempt < max_retries - 1:
                    frappe.msgprint(_("Rate limit exceeded. Retrying in {} seconds...").format(retry_delay))
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    frappe.msgprint(_("Rate limit exceeded. Please try again later."))
                    return
            elif res.status_code != 200:
                frappe.msgprint(_("API request failed with status code: {}").format(res.status_code))
                return
            else:
                break  # If we didn't get a 429 or other error, break out of the retry loop
        
        if res.text:
            try:
                response_data = json.loads(res.text)
            except json.JSONDecodeError:
                frappe.msgprint(_("Failed to parse API response as JSON."))
                frappe.log_error(f"Raw response: {res.text[:1000]}", "India Mart API Response Error")
                return

            if not isinstance(response_data, dict):
                frappe.msgprint(_("Unexpected response type. Expected dict."))
                frappe.log_error(f"Unexpected response type: {type(response_data)}. Response: {json.dumps(response_data, indent=2)}", "India Mart API Response Error")
                return

            if response_data.get("CODE") != 200:
                frappe.msgprint(_("API returned non-success code: {}").format(response_data.get("CODE")))
                frappe.log_error(f"API Error: {response_data.get('MESSAGE', 'No message provided')}", "India Mart API Error")
                return

            leads_data = response_data.get("RESPONSE", [])
            if not isinstance(leads_data, list):
                frappe.msgprint(_("Unexpected RESPONSE type. Expected list."))
                frappe.log_error(f"Unexpected RESPONSE type: {type(leads_data)}. RESPONSE: {json.dumps(leads_data, indent=2)}", "India Mart API Response Error")
                return

            total_records = response_data.get("TOTAL_RECORDS", 0)
            frappe.msgprint(_("Total records received: {}").format(total_records))
            
            leads_created = 0
            leads_existing = 0
            leads_failed = 0
            
            for lead in leads_data:
                result = add_lead(lead)
                if result == True:
                    leads_created += 1
                elif result == "EXISTS":
                    leads_existing += 1
                else:
                    leads_failed += 1
                    frappe.log_error(f"Failed to add lead: {lead.get('UNIQUE_QUERY_ID', 'Unknown ID')}", "India Mart Lead Creation Error")
            
            frappe.msgprint(_("Sync Results:\nCreated: {}\nAlready Existing: {}\nFailed: {}").format(
                leads_created, leads_existing, leads_failed))
            
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("India Mart Sync Error"))
        frappe.throw(_("An error occurred while syncing leads: {}").format(str(e)))

def get_request_url(india_mart_setting, from_date, to_date):
    return (f"{india_mart_setting.url}?"
            f"glusr_crm_key={india_mart_setting.key}&"
            f"start_time={from_date}&"
            f"end_time={to_date}")

@frappe.whitelist()
def cron_sync_lead():
    try:
        today = frappe.utils.today()
        sync_india_mart_lead(today, today)
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("India Mart Sync Error"))
        frappe.throw(_("An error occurred during cron sync. Please check the error log."))

@frappe.whitelist()
def add_lead(lead_data):
    try:
        if not frappe.db.exists("Lead", {"custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]}):
            if not frappe.has_permission("Lead", "create"):
                frappe.throw(_("Not permitted to create Lead"), frappe.PermissionError)
            
            doc = frappe.get_doc({
                "doctype": "Lead",
                "first_name": lead_data["SENDER_NAME"],
                "mobile_no": lead_data["SENDER_MOBILE"],
                "email_id": lead_data["SENDER_EMAIL"],
                "company_name": lead_data.get("SENDER_COMPANY", ""),
                "source": "India Mart",
                "custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"],
                "address_line1": lead_data.get("SENDER_ADDRESS", ""),
                "city": lead_data.get("SENDER_CITY", ""),
                "state": lead_data.get("SENDER_STATE", ""),
                "country": lead_data.get("SENDER_COUNTRY_ISO", ""),
                "pincode": lead_data.get("SENDER_PINCODE", ""),
                "custom_product_name": lead_data.get("QUERY_PRODUCT_NAME", ""),
                "custom_query_message": lead_data.get("QUERY_MESSAGE", ""),
                "custom_query_time": lead_data.get("QUERY_TIME", "")
            })
            doc.insert()
            frappe.db.commit()
            return True
        return "EXISTS"
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Error adding lead from India Mart: {}").format(str(e)))
        return False
