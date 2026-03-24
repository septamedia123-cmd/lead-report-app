import io
import smtplib
import zipfile
import pandas as pd
import streamlit as st
import gspread
from email.message import EmailMessage
from google.oauth2.service_account import Credentials

st.set_page_config(
    page_title="LIVMED Report Portal",
    page_icon="📊",
    layout="wide"
)

CC_EMAIL = "erica@livmed.us"
LOGO_FILE = "logo-fixed.png"
GSHEET_ID = st.secrets["GSHEET_ID"]

LOG_HEADERS = [
    "Timestamp",
    "ReportType",
    "TotalRows",
    "PayableLeads",
    "Centers",
    "EmailsSent",
    "Mode",
    "MissingEmails"
]

CENTER_LOG_HEADERS = [
    "Timestamp",
    "ReportType",
    "CenterName",
    "Identifier",
    "TotalRows",
    "PayableLeads",
    "Email",
    "MissingEmail",
    "Mode"
]

ERROR_LOG_HEADERS = [
    "Timestamp",
    "ReportType",
    "ErrorType",
    "Details",
    "Count",
    "Mode"
]

CENTER_PROFILE_HEADERS = [
    "CenterName",
    "Country",
    "Address",
    "Phone",
    "ContactPerson",
    "CommunicationPreference",
    "TeamEmail",
    "Campaign",
    "PaymentSource",
    "PaymentEmail",
    "PaymentDetails",
    "CGMIdentifier",
    "CGMDID",
    "ECPIdentifier",
    "ECPDID",
    "BGMIdentifier",
    "BGMDID",
    "MAIdentifier",
    "MADID",
    "Notes",
    "Active"
]


# =========================
# HELPERS
# =========================
def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_identifier(value):
    return normalize_text(value)


def find_column(df, candidates):
    lower_map = {str(col).strip().lower(): col for col in df.columns}

    for candidate in candidates:
        c = candidate.lower()
        if c in lower_map:
            return lower_map[c]

    for candidate in candidates:
        c = candidate.lower()
        for col in df.columns:
            if c in str(col).strip().lower():
                return col

    return None


def sanitize_filename(text):
    text = normalize_text(text)
    if not text:
        return "Unknown"
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in text)


def build_email_subject(center_name, identifier, report_name):
    center_name = normalize_text(center_name) or "Center"
    identifier = normalize_text(identifier)
    return f"{report_name} - {center_name} - {identifier}"


def build_email_body(center_name, identifier, total_rows, payable_y, report_name):
    center_name = normalize_text(center_name) or "Team"
    identifier = normalize_text(identifier)

    return f"""Hello {center_name},

Attached is your {report_name.lower()}.

Identifier: {identifier}
Total Records: {total_rows}
Payable Leads: {payable_y}

Please review and let us know if you have any questions.

Best,
Dean
"""


def metric_card(label, value):
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 18px 16px;
            box-shadow: 0 4px 14px rgba(0,0,0,0.05);">
            <div style="font-size: 12px; color: #6b7280; font-weight: 700; text-transform: uppercase; letter-spacing: .04em;">{label}</div>
            <div style="font-size: 28px; color: #111827; font-weight: 800; margin-top: 8px;">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def show_mode_banner(test_mode):
    if test_mode:
        st.markdown(
            """
            <div style="
                background: linear-gradient(90deg, #fff7d6 0%, #fff2b8 100%);
                border: 1px solid #e6c65c;
                padding: 16px 18px;
                border-radius: 12px;
                margin-bottom: 18px;
                font-weight: 700;
                color: #6b5200;
                font-size: 16px;">
                TEST MODE ACTIVE — All emails will be sent only to the test email address.
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <div style="
                background: linear-gradient(90deg, #ffe0e0 0%, #ffd0d0 100%);
                border: 1px solid #df7b7b;
                padding: 16px 18px;
                border-radius: 12px;
                margin-bottom: 18px;
                font-weight: 800;
                color: #8b0000;
                font-size: 16px;">
                LIVE MODE ACTIVE — Emails will be sent to actual vendor addresses.
            </div>
            """,
            unsafe_allow_html=True,
        )


def show_top_header():
    left, right = st.columns([1, 4])

    with left:
        st.image(LOGO_FILE, width=180)

    with right:
        st.markdown(
            """
            <div style="padding-top: 20px;">
                <div style="font-size: 34px; font-weight: 800; color: #111827;">
                    LIVMED Report Portal
                </div>
                <div style="font-size: 16px; color: #6b7280; margin-top: 4px;">
                    Secure report processing, center profiles, file splitting, and vendor delivery
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<hr style='margin-top: 10px; margin-bottom: 20px;'>", unsafe_allow_html=True)


def go_to(page_name):
    st.session_state.current_page = page_name
    st.rerun()


def apply_date_filter(df, option):
    if df.empty or "Timestamp" not in df.columns:
        return df

    now = pd.Timestamp.now()

    if option == "Last 7 days":
        cutoff = now - pd.Timedelta(days=7)
        return df[df["Timestamp"] >= cutoff]

    if option == "Last 30 days":
        cutoff = now - pd.Timedelta(days=30)
        return df[df["Timestamp"] >= cutoff]

    return df


# =========================
# GOOGLE SHEETS
# =========================
def get_gsheet_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=scopes
    )
    return gspread.authorize(creds)


def get_spreadsheet():
    gc = get_gsheet_client()
    return gc.open_by_key(GSHEET_ID)


def get_or_create_worksheet(title, headers, rows=1000, cols=40):
    spreadsheet = get_spreadsheet()
    try:
        worksheet = spreadsheet.worksheet(title)
    except Exception:
        worksheet = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
        worksheet.append_row(headers, value_input_option="USER_ENTERED")

    values = worksheet.get_all_values()
    if not values:
        worksheet.append_row(headers, value_input_option="USER_ENTERED")

    return worksheet


def load_sheet_as_df(title, expected_cols):
    try:
        worksheet = get_or_create_worksheet(title, expected_cols)
        values = worksheet.get_all_values()

        if not values or len(values) < 2:
            return pd.DataFrame(columns=expected_cols)

        headers = values[0]
        rows = values[1:]

        seen = {}
        unique_headers = []
        for h in headers:
            h = str(h).strip()
            if h in seen:
                seen[h] += 1
                unique_headers.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                unique_headers.append(h)

        df = pd.DataFrame(rows, columns=unique_headers)

        rename_map = {}
        for col in df.columns:
            clean = str(col).strip()
            for expected in expected_cols:
                if clean.startswith(expected):
                    rename_map[col] = expected
                    break

        df = df.rename(columns=rename_map)

        for col in expected_cols:
            if col not in df.columns:
                df[col] = ""

        return df[expected_cols]

    except Exception as e:
        st.warning(f"Could not load worksheet '{title}': {e}")
        return pd.DataFrame(columns=expected_cols)


def write_dataframe_to_sheet(title, df, headers):
    worksheet = get_or_create_worksheet(title, headers)

    clean = df.copy()
    for col in headers:
        if col not in clean.columns:
            clean[col] = ""
    clean = clean[headers].fillna("")

    worksheet.clear()
    data = [headers] + clean.astype(str).values.tolist()
    worksheet.update(data, value_input_option="USER_ENTERED")


def log_report_run(report_type, total_rows, payable_leads, centers, emails_sent, mode, missing_emails):
    worksheet = get_or_create_worksheet("logs", LOG_HEADERS)
    row = [
        pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        report_type,
        total_rows,
        payable_leads,
        centers,
        emails_sent,
        mode,
        missing_emails
    ]
    worksheet.append_row(row, value_input_option="USER_ENTERED")


def log_center_runs(report_type, summary_df, mode):
    worksheet = get_or_create_worksheet("center_logs", CENTER_LOG_HEADERS)
    rows = []
    ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    for _, row in summary_df.iterrows():
        rows.append([
            ts,
            report_type,
            normalize_text(row.get("CenterName", "")),
            normalize_text(row.get("Identifier", "")),
            int(pd.to_numeric(row.get("TotalRows", 0), errors="coerce") or 0),
            int(pd.to_numeric(row.get("PayableY", 0), errors="coerce") or 0),
            normalize_text(row.get("Email", "")),
            1 if normalize_text(row.get("Email", "")) == "" else 0,
            mode
        ])

    if rows:
        worksheet.append_rows(rows, value_input_option="USER_ENTERED")


def log_error_event(report_type, error_type, details, count, mode):
    worksheet = get_or_create_worksheet("error_logs", ERROR_LOG_HEADERS)
    row = [
        pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        report_type,
        error_type,
        details,
        count,
        mode
    ]
    worksheet.append_row(row, value_input_option="USER_ENTERED")


def load_dashboard_logs():
    df = load_sheet_as_df("logs", LOG_HEADERS)
    if "Timestamp" in df.columns:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    for col in ["TotalRows", "PayableLeads", "Centers", "EmailsSent", "MissingEmails"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def load_center_logs():
    df = load_sheet_as_df("center_logs", CENTER_LOG_HEADERS)
    if "Timestamp" in df.columns:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    for col in ["TotalRows", "PayableLeads", "MissingEmail"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def load_error_logs():
    df = load_sheet_as_df("error_logs", ERROR_LOG_HEADERS)
    if "Timestamp" in df.columns:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    if "Count" in df.columns:
        df["Count"] = pd.to_numeric(df["Count"], errors="coerce").fillna(0)
    return df


def load_center_profiles():
    df = load_sheet_as_df("center_profiles", CENTER_PROFILE_HEADERS)
    for col in CENTER_PROFILE_HEADERS:
        if col in df.columns:
            df[col] = df[col].apply(normalize_text)
    return df


def save_center_profiles(df):
    write_dataframe_to_sheet("center_profiles", df, CENTER_PROFILE_HEADERS)


# =========================
# CENTER PROFILE LOOKUPS
# =========================
def build_profile_lookup(profiles_df, id_col_name):
    if profiles_df.empty or id_col_name not in profiles_df.columns:
        return pd.DataFrame(columns=["Identifier", "CenterName", "Email", "ProfileNotes", "ContactPerson"])

    lookup = pd.DataFrame({
        "Identifier": profiles_df[id_col_name].apply(normalize_identifier),
        "CenterName": profiles_df["CenterName"].apply(normalize_text) if "CenterName" in profiles_df.columns else "",
        "Email": profiles_df["TeamEmail"].apply(normalize_text) if "TeamEmail" in profiles_df.columns else "",
        "ProfileNotes": profiles_df["Notes"].apply(normalize_text) if "Notes" in profiles_df.columns else "",
        "ContactPerson": profiles_df["ContactPerson"].apply(normalize_text) if "ContactPerson" in profiles_df.columns else "",
    })

    lookup = lookup[lookup["Identifier"] != ""]
    lookup = lookup.drop_duplicates(subset=["Identifier"])
    return lookup


def merge_with_profile_lookup(base_df, identifier_col, profile_lookup):
    base = base_df.copy()
    base["Identifier_normalized"] = base[identifier_col].apply(normalize_identifier)

    if profile_lookup is None or profile_lookup.empty:
        base["FinalCenterName"] = ""
        base["FinalEmail"] = ""
        base["ProfileNotes"] = ""
        base["ContactPerson"] = ""
        return base

    merged = base.merge(
        profile_lookup,
        left_on="Identifier_normalized",
        right_on="Identifier",
        how="left"
    )

    merged["FinalCenterName"] = merged["CenterName"].fillna("").apply(normalize_text)
    merged["FinalEmail"] = merged["Email"].fillna("").apply(normalize_text)
    merged["ProfileNotes"] = merged["ProfileNotes"].fillna("").apply(normalize_text)
    merged["ContactPerson"] = merged["ContactPerson"].fillna("").apply(normalize_text)

    return merged


# =========================
# FILE / EMAIL HELPERS
# =========================
def send_vendor_emails(vendor_files, sender_email, gmail_app_password, test_mode, test_email, report_name):
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(sender_email, gmail_app_password)

    sent_count = 0

    for item in vendor_files:
        if not test_mode and not item["Email"]:
            continue

        to_email = test_email.strip() if test_mode else item["Email"]

        msg = EmailMessage()
        msg["Subject"] = build_email_subject(item["CenterName"], item["Identifier"], report_name)
        msg["From"] = sender_email
        msg["To"] = to_email
        msg["CC"] = item["CC"]
        msg.set_content(
            build_email_body(
                item["CenterName"],
                item["Identifier"],
                item["TotalRows"],
                item["PayableY"],
                report_name
            )
        )

        msg.add_attachment(
            item["CSVBytes"],
            maintype="text",
            subtype="csv",
            filename=item["FileName"]
        )

        server.send_message(msg)
        sent_count += 1

    server.quit()
    return sent_count


def get_file_extension(uploaded_file):
    name = uploaded_file.name.lower()
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[1]


def validate_uploaded_file(uploaded_file, expected_type, report_name):
    ext = get_file_extension(uploaded_file)

    if expected_type == "excel" and ext not in ["xlsx", "xls"]:
        st.error(f"{report_name} requires an Excel file (.xlsx or .xls). You uploaded: {uploaded_file.name}")
        st.info("Please upload the original Excel workbook for this report.")
        return False

    if expected_type == "csv" and ext != "csv":
        st.error(f"{report_name} requires a CSV file (.csv). You uploaded: {uploaded_file.name}")
        st.info("Please upload the CSV export for this report.")
        return False

    return True


def render_upload_instructions(report_name, expected_type):
    if expected_type == "excel":
        st.info(
            f"Upload the **{report_name} Excel workbook** in `.xlsx` or `.xls` format. "
            "For CGM and Med Advantage, the workbook should include the required tabs."
        )
    else:
        st.info(f"Upload the **{report_name} CSV file** in `.csv` format.")


def render_excel_tab_requirements(report_name):
    if report_name in ["CGM Report", "Med Advantage Report"]:
        st.caption("Expected workbook tabs: **Detail** and ideally **Conversion Stats**.")


# =========================
# DASHBOARD / UI
# =========================
def dashboard_card(title, subtitle, button_text, page_key):
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 24px;
            min-height: 180px;
            box-shadow: 0 6px 18px rgba(0,0,0,0.05);">
            <div style="font-size: 22px; font-weight: 800; color: #111827;">{title}</div>
            <div style="font-size: 14px; color: #6b7280; margin-top: 8px;">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    if st.button(button_text, key=f"btn_{page_key}", width="stretch"):
        go_to(page_key)


def render_dashboard():
    logs_df = load_dashboard_logs()
    center_logs_df = load_center_logs()
    error_logs_df = load_error_logs()

    st.markdown(
        """
        <div style="font-size: 28px; font-weight: 800; color: #111827; margin-bottom: 6px;">
            Dashboard
        </div>
        <div style="font-size: 15px; color: #6b7280; margin-bottom: 24px;">
            Live performance across all report workflows.
        </div>
        """,
        unsafe_allow_html=True
    )

    filter_col1, _ = st.columns([1, 5])
    with filter_col1:
        date_filter = st.selectbox("Date Range", ["All time", "Last 7 days", "Last 30 days"], index=0)

    logs_filtered = apply_date_filter(logs_df, date_filter)
    center_logs_filtered = apply_date_filter(center_logs_df, date_filter)
    error_logs_filtered = apply_date_filter(error_logs_df, date_filter)

    total_runs = int(len(logs_filtered)) if not logs_filtered.empty else 0
    total_emails = int(logs_filtered["EmailsSent"].sum()) if not logs_filtered.empty else 0
    total_payable = int(logs_filtered["PayableLeads"].sum()) if not logs_filtered.empty else 0
    total_leads = int(logs_filtered["TotalRows"].sum()) if not logs_filtered.empty else 0
    total_missing_emails = int(logs_filtered["MissingEmails"].sum()) if not logs_filtered.empty else 0
    conversion_rate = f"{(total_payable / total_leads * 100):.1f}%" if total_leads > 0 else "0.0%"
    last_run = "N/A"
    if not logs_filtered.empty and logs_filtered["Timestamp"].notna().any():
        last_run = logs_filtered["Timestamp"].max().strftime("%Y-%m-%d %H:%M")

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    with m1:
        metric_card("Total Runs", total_runs)
    with m2:
        metric_card("Total Leads", total_leads)
    with m3:
        metric_card("Payable Leads", total_payable)
    with m4:
        metric_card("Conversion Rate", conversion_rate)
    with m5:
        metric_card("Emails Sent", total_emails)
    with m6:
        metric_card("Last Run", last_run)

    st.markdown("### Workflows")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        dashboard_card("CGM Report", "Upload Excel, split by LeadSource, and send vendor files.", "Open CGM", "cgm")
    with c2:
        dashboard_card("ECP Report", "Upload CSV, calculate payable, split by Sub Id, and send.", "Open ECP", "ecp")
    with c3:
        dashboard_card("Med Advantage", "Upload Excel, split by LeadSource, and send vendor files.", "Open Med Adv", "medadv")
    with c4:
        dashboard_card("Center Profiles", "Manage master center cards, identifiers, contact info, and notes.", "Open Profiles", "profiles")

    if logs_filtered.empty:
        st.info("No dashboard history found yet for this date range. Send at least one report to populate analytics.")
        return

    st.markdown("### Activity Over Time")
    trend_df = logs_filtered.copy()
    trend_df["Date"] = trend_df["Timestamp"].dt.date
    daily = trend_df.groupby("Date")[["TotalRows", "PayableLeads", "EmailsSent"]].sum()
    st.line_chart(daily, width="stretch")

    ch1, ch2 = st.columns(2)
    with ch1:
        st.markdown("### Runs by Report Type")
        runs_by_report = logs_filtered.groupby("ReportType").size()
        st.bar_chart(runs_by_report, width="stretch")
    with ch2:
        st.markdown("### Emails Sent by Report")
        emails_by_report = logs_filtered.groupby("ReportType")["EmailsSent"].sum()
        st.bar_chart(emails_by_report, width="stretch")

    st.markdown("### Top Performing Centers")
    if center_logs_filtered.empty:
        st.info("No center-level history available yet.")
    else:
        top_centers = (
            center_logs_filtered.groupby("CenterName")[["TotalRows", "PayableLeads"]]
            .sum()
            .reset_index()
            .sort_values(by=["PayableLeads", "TotalRows"], ascending=False)
            .head(10)
        )
        st.dataframe(top_centers, width="stretch")

    st.markdown("### Error Tracking")
    e1, e2, e3 = st.columns(3)
    with e1:
        metric_card("Missing Emails", total_missing_emails)
    with e2:
        recent_error_count = int(error_logs_filtered["Count"].sum()) if not error_logs_filtered.empty else 0
        metric_card("Logged Errors", recent_error_count)
    with e3:
        live_runs = int((logs_filtered["Mode"] == "LIVE").sum()) if "Mode" in logs_filtered.columns else 0
        metric_card("Live Runs", live_runs)

    if not error_logs_filtered.empty:
        st.markdown("### Recent Errors")
        recent_errors = error_logs_filtered.sort_values(by="Timestamp", ascending=False).head(10)
        st.dataframe(recent_errors, width="stretch")

    st.markdown("### Recent Activity")
    recent_cols = ["Timestamp", "ReportType", "TotalRows", "PayableLeads", "Centers", "EmailsSent", "Mode", "MissingEmails"]
    recent_df = logs_filtered.sort_values(by="Timestamp", ascending=False).head(10)[recent_cols]
    st.dataframe(recent_df, width="stretch")


def render_center_profiles_page():
    st.markdown(
        """
        <div style="font-size: 28px; font-weight: 800; color: #111827;">
            Center Profiles
        </div>
        <div style="font-size: 15px; color: #6b7280; margin-top: 4px; margin-bottom: 16px;">
            Master directory for all call centers, contacts, notes, and report identifiers.
        </div>
        """,
        unsafe_allow_html=True
    )

    profiles_df = load_center_profiles()

    st.markdown("### Add New Center")

    a1, a2, a3 = st.columns(3)

    with a1:
        new_center_name = st.text_input("Center Name")
        new_country = st.text_input("Country")
        new_contact = st.text_input("Contact Person")
        new_team_email = st.text_input("Team Email")
        new_phone = st.text_input("Phone")

    with a2:
        new_cgm_id = st.text_input("CGM Identifier")
        new_ecp_id = st.text_input("ECP Identifier")
        new_ma_id = st.text_input("MA Identifier")
        new_campaign = st.text_input("Campaign")
        new_comm_pref = st.text_input("Communication Preference")

    with a3:
        new_payment_source = st.text_input("Payment Source")
        new_payment_email = st.text_input("Payment Email")
        new_payment_details = st.text_input("Payment Details")
        new_notes = st.text_area("Notes")
        new_active = st.selectbox("Active", ["Yes", "No"], index=0)

    if st.button("Save New Center", type="primary", width="stretch"):
        if not new_center_name.strip():
            st.error("Center Name is required.")
        else:
            new_row = {col: "" for col in CENTER_PROFILE_HEADERS}
            new_row["CenterName"] = new_center_name
            new_row["Country"] = new_country
            new_row["ContactPerson"] = new_contact
            new_row["TeamEmail"] = new_team_email
            new_row["Phone"] = new_phone
            new_row["CGMIdentifier"] = new_cgm_id
            new_row["ECPIdentifier"] = new_ecp_id
            new_row["MAIdentifier"] = new_ma_id
            new_row["Campaign"] = new_campaign
            new_row["CommunicationPreference"] = new_comm_pref
            new_row["PaymentSource"] = new_payment_source
            new_row["PaymentEmail"] = new_payment_email
            new_row["PaymentDetails"] = new_payment_details
            new_row["Notes"] = new_notes
            new_row["Active"] = new_active

            updated_df = pd.concat([profiles_df, pd.DataFrame([new_row])], ignore_index=True)
            updated_df = updated_df.fillna("")
            updated_df["CenterName"] = updated_df["CenterName"].apply(normalize_text)
            updated_df = updated_df[updated_df["CenterName"] != ""]
            updated_df = updated_df.drop_duplicates(subset=["CenterName"], keep="last")
            save_center_profiles(updated_df)
            st.success("New center saved.")
            st.rerun()

    st.markdown("### Center Directory")

    search_text = st.text_input("Search centers")
    filtered_df = profiles_df.copy()

    if search_text.strip():
        mask = filtered_df.apply(
            lambda col: col.astype(str).str.contains(search_text, case=False, na=False)
        ).any(axis=1)
        filtered_df = filtered_df[mask]

    if filtered_df.empty:
        st.info("No centers found.")
    else:
        for idx, row in filtered_df.iterrows():
            with st.container():
                st.markdown(
                    f"""
                    <div style="
                        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
                        border: 1px solid #e5e7eb;
                        border-radius: 16px;
                        padding: 18px;
                        margin-bottom: 14px;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.04);">
                        <div style="font-size: 20px; font-weight: 800; color: #111827;">
                            {normalize_text(row.get('CenterName', ''))}
                        </div>
                        <div style="font-size: 13px; color: #6b7280; margin-top: 4px;">
                            {normalize_text(row.get('Country', ''))} • {normalize_text(row.get('ContactPerson', ''))}
                        </div>
                        <div style="margin-top: 8px; font-size: 13px;">
                            📧 {normalize_text(row.get('TeamEmail', ''))}
                        </div>
                        <div style="margin-top: 8px; font-size: 12px; color: #374151;">
                            CGM: {normalize_text(row.get('CGMIdentifier', ''))} |
                            ECP: {normalize_text(row.get('ECPIdentifier', ''))} |
                            MA: {normalize_text(row.get('MAIdentifier', ''))}
                        </div>
                        <div style="margin-top: 8px; font-size: 12px; color: #6b7280;">
                            {normalize_text(row.get('Notes', ''))}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Edit", key=f"edit_center_{idx}", width="stretch"):
                        st.session_state["edit_center_idx"] = idx
                        st.rerun()

                with c2:
                    if st.button("Delete", key=f"delete_center_{idx}", width="stretch"):
                        updated_df = profiles_df.drop(index=idx).reset_index(drop=True)
                        save_center_profiles(updated_df)
                        st.warning("Center deleted.")
                        st.rerun()

    if "edit_center_idx" in st.session_state:
        edit_idx = st.session_state["edit_center_idx"]
        edit_row = profiles_df.loc[edit_idx]

        st.markdown("### Edit Center")

        e1, e2, e3 = st.columns(3)

        with e1:
            edit_center_name = st.text_input("Edit Center Name", value=edit_row.get("CenterName", ""), key="edit_center_name")
            edit_country = st.text_input("Edit Country", value=edit_row.get("Country", ""), key="edit_country")
            edit_contact = st.text_input("Edit Contact Person", value=edit_row.get("ContactPerson", ""), key="edit_contact")
            edit_team_email = st.text_input("Edit Team Email", value=edit_row.get("TeamEmail", ""), key="edit_team_email")
            edit_phone = st.text_input("Edit Phone", value=edit_row.get("Phone", ""), key="edit_phone")

        with e2:
            edit_cgm_id = st.text_input("Edit CGM Identifier", value=edit_row.get("CGMIdentifier", ""), key="edit_cgm")
            edit_ecp_id = st.text_input("Edit ECP Identifier", value=edit_row.get("ECPIdentifier", ""), key="edit_ecp")
            edit_ma_id = st.text_input("Edit MA Identifier", value=edit_row.get("MAIdentifier", ""), key="edit_ma")
            edit_campaign = st.text_input("Edit Campaign", value=edit_row.get("Campaign", ""), key="edit_campaign")
            edit_comm_pref = st.text_input("Edit Communication Preference", value=edit_row.get("CommunicationPreference", ""), key="edit_comm")

        with e3:
            edit_payment_source = st.text_input("Edit Payment Source", value=edit_row.get("PaymentSource", ""), key="edit_pay_source")
            edit_payment_email = st.text_input("Edit Payment Email", value=edit_row.get("PaymentEmail", ""), key="edit_pay_email")
            edit_payment_details = st.text_input("Edit Payment Details", value=edit_row.get("PaymentDetails", ""), key="edit_pay_details")
            edit_notes = st.text_area("Edit Notes", value=edit_row.get("Notes", ""), key="edit_notes")
            edit_active = st.selectbox("Edit Active", ["Yes", "No"], index=0 if edit_row.get("Active", "Yes") == "Yes" else 1, key="edit_active")

        s1, s2 = st.columns(2)
        with s1:
            if st.button("Save Center Changes", width="stretch"):
                for col in CENTER_PROFILE_HEADERS:
                    if col not in profiles_df.columns:
                        profiles_df[col] = ""

                profiles_df.at[edit_idx, "CenterName"] = edit_center_name
                profiles_df.at[edit_idx, "Country"] = edit_country
                profiles_df.at[edit_idx, "ContactPerson"] = edit_contact
                profiles_df.at[edit_idx, "TeamEmail"] = edit_team_email
                profiles_df.at[edit_idx, "Phone"] = edit_phone
                profiles_df.at[edit_idx, "CGMIdentifier"] = edit_cgm_id
                profiles_df.at[edit_idx, "ECPIdentifier"] = edit_ecp_id
                profiles_df.at[edit_idx, "MAIdentifier"] = edit_ma_id
                profiles_df.at[edit_idx, "Campaign"] = edit_campaign
                profiles_df.at[edit_idx, "CommunicationPreference"] = edit_comm_pref
                profiles_df.at[edit_idx, "PaymentSource"] = edit_payment_source
                profiles_df.at[edit_idx, "PaymentEmail"] = edit_payment_email
                profiles_df.at[edit_idx, "PaymentDetails"] = edit_payment_details
                profiles_df.at[edit_idx, "Notes"] = edit_notes
                profiles_df.at[edit_idx, "Active"] = edit_active

                save_center_profiles(profiles_df)
                st.success("Center updated.")
                del st.session_state["edit_center_idx"]
                st.rerun()

        with s2:
            if st.button("Cancel Edit", width="stretch"):
                del st.session_state["edit_center_idx"]
                st.rerun()

    with st.expander("Advanced Table Editor"):
        edited_df = st.data_editor(profiles_df, width="stretch", num_rows="dynamic", key="profiles_table_editor")
        if st.button("Save Table Changes", width="stretch"):
            save_center_profiles(edited_df)
            st.success("Table changes saved.")
            st.rerun()


# =========================
# REPORT PAGE ENGINE
# =========================
def render_report_page(
    report_key,
    report_name,
    identifier_label,
    profile_identifier_col,
    file_type,
    remove_columns,
    custom_payable_rule=None
):
    back_col, title_col = st.columns([1, 6])

    with back_col:
        if st.button("← Back", key=f"back_{report_key}", width="stretch"):
            go_to("dashboard")

    with title_col:
        st.markdown(
            f"""
            <div style="font-size: 28px; font-weight: 800; color: #111827;">
                {report_name}
            </div>
            <div style="font-size: 15px; color: #6b7280; margin-top: 4px; margin-bottom: 16px;">
                Run report, review results, use center profiles for mapping, and send files.
            </div>
            """,
            unsafe_allow_html=True
        )

    page_tab1, page_tab2 = st.tabs(["Run Report", "Center Profiles"])

    with page_tab2:
        profiles_df = load_center_profiles()
        cols_to_show = ["CenterName", "TeamEmail", profile_identifier_col, "ContactPerson", "Notes", "Active"]
        cols_to_show = [c for c in cols_to_show if c in profiles_df.columns]
        st.subheader(f"{report_name} Profile Mapping View")
        st.dataframe(profiles_df[cols_to_show], width="stretch")
        st.caption(f"This report uses `{profile_identifier_col}` from Center Profiles as its mapping source.")
        if st.button("Open Full Center Profiles", key=f"profiles_jump_{report_key}", width="stretch"):
            go_to("profiles")

    with page_tab1:
        controls_left, controls_mid, controls_right = st.columns([1, 1, 2])

        with controls_left:
            test_mode = st.checkbox("Test Mode", value=True, key=f"{report_key}_test_mode")
        with controls_mid:
            test_email = st.text_input("Test Email", key=f"{report_key}_test_email")
        with controls_right:
            st.info(f"Every email from this workflow is automatically CC'd to: {CC_EMAIL}")

        show_mode_banner(test_mode)
        render_upload_instructions(report_name, file_type)
        render_excel_tab_requirements(report_name)

        uploaded_file = st.file_uploader(
            f"Upload {report_name} file",
            type=["xlsx", "xls"] if file_type == "excel" else ["csv"],
            key=f"{report_key}_uploader"
        )

        if uploaded_file is None:
            st.info(f"Upload a file to begin the {report_name.lower()} workflow.")
            return

        if not validate_uploaded_file(uploaded_file, file_type, report_name):
            return

        try:
            detail_df = pd.DataFrame()
            raw_preview_df = pd.DataFrame()

            if file_type == "excel":
                try:
                    xls = pd.ExcelFile(uploaded_file)
                except Exception as e:
                    st.error(f"Could not open this Excel file: {e}")
                    st.info("Please make sure you uploaded a valid Excel workbook.")
                    return

                detail_sheet = None
                for sheet in xls.sheet_names:
                    if "detail" in sheet.strip().lower():
                        detail_sheet = sheet
                        break

                if detail_sheet is None:
                    st.error("Could not find the Detail sheet in this workbook.")
                    st.info("Please upload the original report workbook that includes a Detail tab.")
                    return

                detail_df = pd.read_excel(uploaded_file, sheet_name=detail_sheet)
                raw_preview_df = detail_df.copy()

            else:
                try:
                    detail_df = pd.read_csv(uploaded_file)
                    raw_preview_df = detail_df.copy()
                except Exception as e:
                    st.error(f"Could not open this CSV file: {e}")
                    st.info("Please make sure you uploaded a valid CSV export.")
                    return

            id_col = find_column(detail_df, [identifier_label, identifier_label.replace(" ", "")])
            if id_col is None:
                st.error(f"Could not find {identifier_label} in the uploaded file.")
                return

            disposition_col = find_column(detail_df, ["Disposition"])
            paidamount_col = find_column(detail_df, ["PaidAmount", "Paid Amount"])

            if custom_payable_rule == "ecp":
                duration_col = find_column(detail_df, ["Duration"])
                if duration_col is None or disposition_col is None:
                    st.error("ECP requires both Duration and Disposition columns to calculate Payable.")
                    return

                detail_df["Payable"] = detail_df.apply(
                    lambda row: "Y"
                    if normalize_text(row[disposition_col]).lower() == "transfer sale"
                    and pd.to_numeric(row[duration_col], errors="coerce") >= 120
                    else "N",
                    axis=1
                )

            profiles_df = load_center_profiles()
            profile_lookup = build_profile_lookup(profiles_df, profile_identifier_col)
            merged_df = merge_with_profile_lookup(detail_df, id_col, profile_lookup)

            summary_rows = []

            for identifier_value, group in merged_df.groupby("Identifier_normalized"):
                payable_y = 0
                payable_n = 0
                total_paid = 0.0

                payable_col = find_column(group, ["Payable"])
                if payable_col:
                    payable_series = group[payable_col].astype(str).str.strip().str.upper()
                    payable_y = int((payable_series == "Y").sum())
                    payable_n = int((payable_series == "N").sum())

                if paidamount_col and paidamount_col in group.columns:
                    total_paid = float(pd.to_numeric(group[paidamount_col], errors="coerce").fillna(0).sum())

                center_name = normalize_text(group["FinalCenterName"].iloc[0]) if "FinalCenterName" in group.columns else ""
                email = normalize_text(group["FinalEmail"].iloc[0]) if "FinalEmail" in group.columns else ""

                summary_rows.append({
                    "Identifier": identifier_value,
                    "CenterName": center_name,
                    "Email": email,
                    "CC": CC_EMAIL,
                    "TotalRows": len(group),
                    "PayableY": payable_y,
                    "PayableN": payable_n,
                    "TotalPaidAmount": total_paid,
                    "ReadyToSend": "Yes" if email else "No"
                })

            summary_df = pd.DataFrame(summary_rows)
            if not summary_df.empty:
                summary_df = summary_df.sort_values(by=["CenterName", "Identifier"], na_position="last")

            if disposition_col and disposition_col in merged_df.columns:
                disposition_summary = (
                    merged_df.groupby(["Identifier_normalized", disposition_col])
                    .size()
                    .reset_index(name="Count")
                    .rename(columns={"Identifier_normalized": "Identifier"})
                )
            else:
                disposition_summary = pd.DataFrame()

            missing_email_df = pd.DataFrame()
            if not summary_df.empty:
                missing_email_df = summary_df[summary_df["Email"].astype(str).str.strip() == ""]

            zip_buffer = io.BytesIO()
            vendor_files = []

            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for identifier_value, group in merged_df.groupby("Identifier_normalized"):
                    center_name = normalize_text(group["FinalCenterName"].iloc[0]) if "FinalCenterName" in group.columns else ""
                    email = normalize_text(group["FinalEmail"].iloc[0]) if "FinalEmail" in group.columns else ""

                    safe_identifier = sanitize_filename(identifier_value)
                    safe_center = sanitize_filename(center_name) if center_name else "UnknownCenter"

                    cols_to_remove = [
                        "Identifier_normalized",
                        "Identifier",
                        "CenterName",
                        "Email",
                        "FinalCenterName",
                        "FinalEmail",
                        "ProfileNotes",
                        "ContactPerson"
                    ] + remove_columns

                    export_group = group.drop(columns=cols_to_remove, errors="ignore")

                    csv_bytes = export_group.to_csv(index=False).encode("utf-8")
                    csv_filename = f"{safe_center}__{safe_identifier}.csv"
                    zip_file.writestr(csv_filename, csv_bytes)

                    payable_col = find_column(group, ["Payable"])
                    payable_y = 0
                    if payable_col and payable_col in group.columns:
                        payable_series = group[payable_col].astype(str).str.strip().str.upper()
                        payable_y = int((payable_series == "Y").sum())

                    vendor_files.append({
                        "Identifier": identifier_value,
                        "CenterName": center_name,
                        "Email": email,
                        "CC": CC_EMAIL,
                        "FileName": csv_filename,
                        "CSVBytes": csv_bytes,
                        "TotalRows": len(group),
                        "PayableY": payable_y
                    })

                zip_file.writestr("center_summary.csv", summary_df.to_csv(index=False).encode("utf-8"))
                if not disposition_summary.empty:
                    zip_file.writestr("disposition_summary.csv", disposition_summary.to_csv(index=False).encode("utf-8"))

            zip_buffer.seek(0)

            total_centers = len(summary_df)
            total_rows = int(summary_df["TotalRows"].sum()) if not summary_df.empty else 0
            total_payable = int(summary_df["PayableY"].sum()) if not summary_df.empty else 0
            ready_count = int((summary_df["ReadyToSend"] == "Yes").sum()) if not summary_df.empty else 0
            conversion_rate = f"{(total_payable / total_rows * 100):.1f}%" if total_rows > 0 else "0.0%"

            m1, m2, m3, m4, m5 = st.columns(5)
            with m1:
                metric_card("Centers", total_centers)
            with m2:
                metric_card("Total Leads", total_rows)
            with m3:
                metric_card("Payable Leads", total_payable)
            with m4:
                metric_card("Conversion Rate", conversion_rate)
            with m5:
                metric_card("Ready to Send", ready_count)

            tab1, tab2, tab3, tab4 = st.tabs(["Review", "Downloads", "Email Queue", "Raw Preview"])

            with tab1:
                st.subheader("Center Summary")
                st.dataframe(summary_df, width="stretch")

                if not missing_email_df.empty:
                    st.warning("Some centers are missing emails in Center Profiles.")
                    st.dataframe(missing_email_df, width="stretch")

                if not disposition_summary.empty:
                    st.subheader("Disposition Summary")
                    st.dataframe(disposition_summary, width="stretch")

            with tab2:
                st.subheader("Downloads")
                st.download_button(
                    label="Download ZIP of all center CSV files",
                    data=zip_buffer,
                    file_name=f"{report_key}_split_reports.zip",
                    mime="application/zip",
                    width="stretch"
                )
                st.download_button(
                    label="Download center summary CSV",
                    data=summary_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{report_key}_center_summary.csv",
                    mime="text/csv",
                    width="stretch"
                )

            with tab3:
                st.subheader("Email Queue")

                email_preview_rows = []
                for item in vendor_files:
                    email_preview_rows.append({
                        "CenterName": item["CenterName"],
                        "ToEmail": test_email.strip() if test_mode else item["Email"],
                        "CC": item["CC"],
                        "File": item["FileName"],
                        "TotalRows": item["TotalRows"],
                        "PayableY": item["PayableY"],
                        "Status": "READY" if (test_mode and test_email.strip()) or item["Email"] else "MISSING EMAIL"
                    })

                email_preview_df = pd.DataFrame(email_preview_rows)
                st.dataframe(email_preview_df, width="stretch")

                live_confirm = False
                if not test_mode:
                    live_confirm = st.checkbox(
                        "I confirm I want to send emails to live vendor addresses.",
                        key=f"{report_key}_live_confirm"
                    )

                send_disabled = False
                if test_mode and not test_email.strip():
                    send_disabled = True
                    st.info("Enter a test email to enable sending in Test Mode.")

                if not test_mode and not live_confirm:
                    send_disabled = True
                    st.warning("Live sending requires confirmation before emails can be sent.")

                if st.button("Send Emails", key=f"{report_key}_send_emails", type="primary", width="stretch", disabled=send_disabled):
                    try:
                        sent_count = send_vendor_emails(
                            vendor_files=vendor_files,
                            sender_email=sender_email,
                            gmail_app_password=gmail_app_password,
                            test_mode=test_mode,
                            test_email=test_email,
                            report_name=report_name
                        )

                        st.success(f"Sent {sent_count} emails successfully.")

                        mode_value = "TEST" if test_mode else "LIVE"

                        try:
                            log_report_run(
                                report_type=report_name,
                                total_rows=total_rows,
                                payable_leads=total_payable,
                                centers=total_centers,
                                emails_sent=sent_count,
                                mode=mode_value,
                                missing_emails=len(missing_email_df)
                            )

                            log_center_runs(
                                report_type=report_name,
                                summary_df=summary_df,
                                mode=mode_value
                            )

                            if len(missing_email_df) > 0:
                                log_error_event(
                                    report_type=report_name,
                                    error_type="Missing Emails",
                                    details=f"{len(missing_email_df)} centers missing emails",
                                    count=len(missing_email_df),
                                    mode=mode_value
                                )

                            st.rerun()

                        except Exception as log_error:
                            st.warning(f"Run completed, but dashboard logging could not be written to Google Sheets: {log_error}")

                    except Exception as e:
                        try:
                            log_error_event(
                                report_type=report_name,
                                error_type="Email Send Failure",
                                details=str(e),
                                count=1,
                                mode="TEST" if test_mode else "LIVE"
                            )
                        except Exception:
                            pass
                        st.error(f"Email error: {e}")

            with tab4:
                st.subheader("Raw Preview")
                st.dataframe(raw_preview_df.head(20), width="stretch")

            st.info(f"This report maps `{identifier_label}` to `{profile_identifier_col}` in Center Profiles.")

        except Exception as e:
            st.error(f"Something went wrong while processing this file: {e}")
            st.info("Please verify that you uploaded the correct report file and that the expected columns are present.")


# =========================
# SESSION + SECRETS
# =========================
sender_email = st.secrets["EMAIL"]
gmail_app_password = st.secrets["PASSWORD"]
site_password = st.secrets["APP_PASSWORD"]

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if "current_page" not in st.session_state:
    st.session_state.current_page = "dashboard"


# =========================
# LOGIN SCREEN
# =========================
if not st.session_state.authenticated:
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #020617 0%, #0f172a 100%);
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown(
            """
            <div style="text-align: center; padding-top: 60px; padding-bottom: 20px;">
            """,
            unsafe_allow_html=True,
        )

        st.image(LOGO_FILE, width=260)

        st.markdown(
            """
            <div style="font-size: 30px; font-weight: 800; color: white; margin-top: 12px;">
                LIVMED Report Portal
            </div>
            <div style="font-size: 15px; color: #cbd5e1; margin-top: 8px; margin-bottom: 20px;">
                Secure access required
            </div>
            """,
            unsafe_allow_html=True,
        )

        entered_password = st.text_input("Access Password", type="password")
        login_clicked = st.button("Login", width="stretch")

        if login_clicked:
            if entered_password == site_password:
                st.session_state.authenticated = True
                st.session_state.current_page = "dashboard"
                st.rerun()
            else:
                st.error("Incorrect password.")

        st.markdown("</div>", unsafe_allow_html=True)

    st.stop()


# =========================
# MAIN HEADER
# =========================
show_top_header()

top_left, top_mid, top_right = st.columns([5, 1, 1])

with top_mid:
    if st.button("Dashboard", width="stretch"):
        st.session_state.current_page = "dashboard"
        st.rerun()

with top_right:
    if st.button("Log out", width="stretch"):
        st.session_state.authenticated = False
        st.session_state.current_page = "dashboard"
        st.rerun()


# =========================
# ROUTER
# =========================
current_page = st.session_state.current_page

if current_page == "dashboard":
    render_dashboard()

elif current_page == "profiles":
    render_center_profiles_page()

elif current_page == "cgm":
    render_report_page(
        report_key="cgm",
        report_name="CGM Report",
        identifier_label="LeadSource",
        profile_identifier_col="CGMIdentifier",
        file_type="excel",
        remove_columns=[
            "PaidAmount",
            "Paid Amount",
            "DiabeticOnMedicare",
            "Diabetic On Medicare",
            "AssignedTo",
            "Assigned To"
        ],
        custom_payable_rule=None
    )

elif current_page == "ecp":
    render_report_page(
        report_key="ecp",
        report_name="ECP Report",
        identifier_label="Sub Id",
        profile_identifier_col="ECPIdentifier",
        file_type="csv",
        remove_columns=[],
        custom_payable_rule="ecp"
    )

elif current_page == "medadv":
    render_report_page(
        report_key="medadv",
        report_name="Med Advantage Report",
        identifier_label="LeadSource",
        profile_identifier_col="MAIdentifier",
        file_type="excel",
        remove_columns=[
            "PaidAmount",
            "Paid Amount",
            "DiabeticOnMedicare",
            "Diabetic On Medicare",
            "AssignedTo",
            "Assigned To"
        ],
        custom_payable_rule=None
    )
