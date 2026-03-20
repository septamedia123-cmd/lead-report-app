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

MAPPING_COLUMNS = ["Identifier", "CenterName", "Email", "Active", "Notes"]


# =========================
# HELPERS
# =========================
def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


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


def empty_mapping_df():
    return pd.DataFrame(columns=MAPPING_COLUMNS)


def ensure_mapping_state(report_key):
    state_key = f"{report_key}_mapping"
    if state_key not in st.session_state:
        st.session_state[state_key] = empty_mapping_df()


def get_mapping_df(report_key):
    ensure_mapping_state(report_key)
    df = st.session_state[f"{report_key}_mapping"].copy()
    for col in MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[MAPPING_COLUMNS].fillna("")


def set_mapping_df(report_key, df):
    clean = df.copy()
    for col in MAPPING_COLUMNS:
        if col not in clean.columns:
            clean[col] = ""
    clean = clean[MAPPING_COLUMNS].fillna("")
    clean["Identifier"] = clean["Identifier"].apply(normalize_text)
    clean["CenterName"] = clean["CenterName"].apply(normalize_text)
    clean["Email"] = clean["Email"].apply(normalize_text)
    clean["Active"] = clean["Active"].apply(normalize_text)
    clean["Notes"] = clean["Notes"].apply(normalize_text)
    clean = clean.drop_duplicates(subset=["Identifier"])
    st.session_state[f"{report_key}_mapping"] = clean


def metric_card(label, value):
    st.markdown(
        f"""
        <div style="
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 14px;
            padding: 18px 16px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);">
            <div style="font-size: 13px; color: #6b7280; font-weight: 600;">{label}</div>
            <div style="font-size: 28px; color: #111827; font-weight: 800; margin-top: 6px;">{value}</div>
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
                    Secure report processing, file splitting, mapping management, and vendor delivery
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<hr style='margin-top: 10px; margin-bottom: 20px;'>", unsafe_allow_html=True)


def merge_with_mapping(base_df, identifier_col, mapping_df, workbook_lookup=None):
    base = base_df.copy()
    base["Identifier_normalized"] = base[identifier_col].apply(normalize_text)

    if workbook_lookup is None:
        workbook_lookup = pd.DataFrame(columns=["Identifier", "CenterName", "Email"])

    if workbook_lookup.empty:
        workbook_lookup = pd.DataFrame(columns=["Identifier", "CenterName", "Email"])

    workbook_lookup = workbook_lookup.copy()
    if not workbook_lookup.empty:
        workbook_lookup["Identifier"] = workbook_lookup["Identifier"].apply(normalize_text)
        workbook_lookup["CenterName"] = workbook_lookup["CenterName"].apply(normalize_text)
        workbook_lookup["Email"] = workbook_lookup["Email"].apply(normalize_text)
        workbook_lookup = workbook_lookup.drop_duplicates(subset=["Identifier"])

    mapping_df = mapping_df.copy()
    if not mapping_df.empty:
        mapping_df["Identifier"] = mapping_df["Identifier"].apply(normalize_text)
        mapping_df["CenterName"] = mapping_df["CenterName"].apply(normalize_text)
        mapping_df["Email"] = mapping_df["Email"].apply(normalize_text)
        mapping_df = mapping_df.drop_duplicates(subset=["Identifier"])

    merged = base.merge(
        workbook_lookup[["Identifier", "CenterName", "Email"]] if not workbook_lookup.empty else pd.DataFrame(columns=["Identifier", "CenterName", "Email"]),
        left_on="Identifier_normalized",
        right_on="Identifier",
        how="left"
    )

    merged = merged.merge(
        mapping_df[["Identifier", "CenterName", "Email"]] if not mapping_df.empty else pd.DataFrame(columns=["Identifier", "CenterName", "Email"]),
        left_on="Identifier_normalized",
        right_on="Identifier",
        how="left",
        suffixes=("_workbook", "_mapping")
    )

    workbook_center_col = "CenterName_workbook" if "CenterName_workbook" in merged.columns else "CenterName"
    workbook_email_col = "Email_workbook" if "Email_workbook" in merged.columns else "Email"
    mapping_center_col = "CenterName_mapping" if "CenterName_mapping" in merged.columns else None
    mapping_email_col = "Email_mapping" if "Email_mapping" in merged.columns else None

    merged["FinalCenterName"] = merged[workbook_center_col] if workbook_center_col in merged.columns else ""
    merged["FinalEmail"] = merged[workbook_email_col] if workbook_email_col in merged.columns else ""

    if mapping_center_col and mapping_center_col in merged.columns:
        merged["FinalCenterName"] = merged[mapping_center_col].where(
            merged[mapping_center_col].astype(str).str.strip() != "",
            merged["FinalCenterName"]
        )

    if mapping_email_col and mapping_email_col in merged.columns:
        merged["FinalEmail"] = merged[mapping_email_col].where(
            merged[mapping_email_col].astype(str).str.strip() != "",
            merged["FinalEmail"]
        )

    merged["FinalCenterName"] = merged["FinalCenterName"].fillna("").apply(normalize_text)
    merged["FinalEmail"] = merged["FinalEmail"].fillna("").apply(normalize_text)

    return merged


def build_workbook_lookup(conversion_df):
    if conversion_df is None or conversion_df.empty:
        return pd.DataFrame(columns=["Identifier", "CenterName", "Email"])

    identifier_col = conversion_df.columns[0]
    center_name_col = find_column(conversion_df, ["Center Name", "CenterName"])
    email_col = find_column(conversion_df, ["Email", "Email Address"])

    lookup = pd.DataFrame({
        "Identifier": conversion_df[identifier_col].apply(normalize_text),
        "CenterName": conversion_df[center_name_col].apply(normalize_text) if center_name_col else "",
        "Email": conversion_df[email_col].apply(normalize_text) if email_col else "",
    })

    lookup = lookup.drop_duplicates(subset=["Identifier"])
    lookup = lookup[lookup["Identifier"] != ""]
    return lookup


def render_mapping_manager(report_key, report_name, identifier_label):
    st.subheader(f"{report_name} Mappings")
    st.info(
        "Use this mapping table to manage identifiers, center names, and email addresses. "
        "Download the CSV after editing to keep a backup."
    )

    uploaded_mapping = st.file_uploader(
        f"Upload {report_name} mapping CSV",
        type=["csv"],
        key=f"{report_key}_mapping_upload"
    )

    if uploaded_mapping is not None:
        try:
            uploaded_df = pd.read_csv(uploaded_mapping)
            rename_map = {}
            for col in uploaded_df.columns:
                col_lower = str(col).strip().lower()
                if col_lower in ["subid", "sub id", "leadsource", "lead source", "identifier"]:
                    rename_map[col] = "Identifier"
                elif col_lower in ["centername", "center name"]:
                    rename_map[col] = "CenterName"
                elif col_lower in ["email", "email address"]:
                    rename_map[col] = "Email"
                elif col_lower == "active":
                    rename_map[col] = "Active"
                elif col_lower == "notes":
                    rename_map[col] = "Notes"

            uploaded_df = uploaded_df.rename(columns=rename_map)
            for col in MAPPING_COLUMNS:
                if col not in uploaded_df.columns:
                    uploaded_df[col] = ""
            set_mapping_df(report_key, uploaded_df[MAPPING_COLUMNS])
            st.success("Mapping file loaded into the app.")
        except Exception as e:
            st.error(f"Could not load mapping file: {e}")

    mapping_df = get_mapping_df(report_key)

    edited_df = st.data_editor(
        mapping_df,
        width="stretch",
        num_rows="dynamic",
        key=f"{report_key}_editor"
    )

    c1, c2 = st.columns(2)
    with c1:
        if st.button(f"Save {report_name} Mapping Changes", key=f"{report_key}_save_mapping", use_container_width=True):
            set_mapping_df(report_key, edited_df)
            st.success("Mapping changes saved.")
    with c2:
        st.download_button(
            label=f"Download {report_name} Mapping CSV",
            data=edited_df.to_csv(index=False).encode("utf-8"),
            file_name=f"{report_key}_mapping.csv",
            mime="text/csv",
            use_container_width=True
        )

    st.caption(f"Identifier for this report: {identifier_label}")


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


def log_report_run(
    report_type,
    total_rows,
    payable_leads,
    centers,
    emails_sent,
    mode,
    missing_emails
):
    try:
        gc = get_gsheet_client()
        sheet = gc.open("LIVMED Report Logs").worksheet("logs")

        new_row = [
            pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            report_type,
            total_rows,
            payable_leads,
            centers,
            emails_sent,
            mode,
            missing_emails
        ]

        sheet.append_row(new_row, value_input_option="USER_ENTERED")
        st.success("Run successfully logged to Google Sheets.")

    except Exception as e:
        st.warning(f"Run completed, but log could not be written to Google Sheets: {str(e)}")

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

    if expected_type == "excel":
        if ext not in ["xlsx", "xls"]:
            st.error(
                f"{report_name} requires an Excel file (.xlsx or .xls). "
                f"You uploaded: {uploaded_file.name}"
            )
            st.info("Please go back and upload the original Excel workbook for this report.")
            return False

    if expected_type == "csv":
        if ext != "csv":
            st.error(
                f"{report_name} requires a CSV file (.csv). "
                f"You uploaded: {uploaded_file.name}"
            )
            st.info("Please go back and upload the CSV export for this report.")
            return False

    return True


def render_upload_instructions(report_name, expected_type):
    if expected_type == "excel":
        st.info(
            f"Upload the **{report_name} Excel workbook** in `.xlsx` or `.xls` format. "
            "For CGM and Med Advantage, the workbook should include the required report tabs."
        )
    else:
        st.info(
            f"Upload the **{report_name} CSV file** in `.csv` format."
        )


def render_excel_tab_requirements(report_name):
    if report_name in ["CGM Report", "Med Advantage Report"]:
        st.caption("Expected workbook tabs: **Detail** and ideally **Conversion Stats**.")


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

for key in ["cgm", "ecp", "medadv"]:
    ensure_mapping_state(key)


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
            <div style="
                text-align: center;
                padding-top: 60px;
                padding-bottom: 20px;">
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
        login_clicked = st.button("Login", use_container_width=True)

        if login_clicked:
            if entered_password == site_password:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password.")

        st.markdown("</div>", unsafe_allow_html=True)

    st.stop()


# =========================
# MAIN HEADER
# =========================
show_top_header()

top_left, top_right = st.columns([6, 1])
with top_right:
    if st.button("Log out", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()


# =========================
# DASHBOARD
# =========================
def go_to(page_name):
    st.session_state.current_page = page_name
    st.rerun()


def dashboard_card(title, subtitle, button_text, page_key):
    st.markdown(
        f"""
        <div style="
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 24px;
            min-height: 180px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.04);">
            <div style="font-size: 22px; font-weight: 800; color: #111827;">{title}</div>
            <div style="font-size: 14px; color: #6b7280; margin-top: 8px;">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True
    )
    if st.button(button_text, key=f"btn_{page_key}", use_container_width=True):
        go_to(page_key)


def render_dashboard():
    st.markdown(
        """
        <div style="font-size: 28px; font-weight: 800; color: #111827; margin-bottom: 6px;">
            Dashboard
        </div>
        <div style="font-size: 15px; color: #6b7280; margin-bottom: 24px;">
            Choose a report workflow to run or manage.
        </div>
        """,
        unsafe_allow_html=True
    )

    m1, m2, m3 = st.columns(3)
    with m1:
        metric_card("Available Workflows", 3)
    with m2:
        metric_card("Email CC", "Erica")
    with m3:
        metric_card("Portal Status", "Live")

    st.markdown("### Report Workflows")

    c1, c2, c3 = st.columns(3)
    with c1:
        dashboard_card(
            "CGM Report",
            "Upload daily Excel workbook, split by LeadSource, manage mappings, and email vendor files.",
            "Open CGM Report",
            "cgm"
        )
    with c2:
        dashboard_card(
            "ECP Report",
            "Upload CSV, calculate Payable using Transfer Sale + Duration >= 120, split by Sub Id, and email vendor files.",
            "Open ECP Report",
            "ecp"
        )
    with c3:
        dashboard_card(
            "Med Advantage Report",
            "Upload Excel workbook, split by LeadSource, keep columns N and O, remove columns K/L/M, and email vendor files.",
            "Open Med Advantage Report",
            "medadv"
        )


# =========================
# REPORT PAGES
# =========================
def render_report_page(report_key, report_name, identifier_label, file_type, needs_conversion_stats, remove_columns, custom_payable_rule=None):
    back_col, title_col = st.columns([1, 6])

    with back_col:
        if st.button("← Back", key=f"back_{report_key}", use_container_width=True):
            go_to("dashboard")

    with title_col:
        st.markdown(
            f"""
            <div style="font-size: 28px; font-weight: 800; color: #111827;">
                {report_name}
            </div>
            <div style="font-size: 15px; color: #6b7280; margin-top: 4px; margin-bottom: 16px;">
                Run report, review results, manage mappings, and send files.
            </div>
            """,
            unsafe_allow_html=True
        )

    page_tab1, page_tab2 = st.tabs(["Run Report", "Manage Mappings"])

    with page_tab2:
        render_mapping_manager(report_key, report_name, identifier_label)

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
            workbook_lookup = pd.DataFrame(columns=["Identifier", "CenterName", "Email"])
            detail_df = pd.DataFrame()
            conversion_df = pd.DataFrame()
            disposition_col = None
            paidamount_col = None

            if file_type == "excel":
                try:
                    xls = pd.ExcelFile(uploaded_file)
                except ImportError:
                    st.error(
                        "This app needs the Excel package `openpyxl` installed to read Excel files. "
                        "Please update `requirements.txt` to include `openpyxl` and reboot the app."
                    )
                    return
                except Exception as e:
                    st.error(f"Could not open this Excel file: {e}")
                    st.info("Please make sure you uploaded a valid Excel workbook.")
                    return

                sheet_names = xls.sheet_names

                detail_sheet = None
                conversion_sheet = None

                for sheet in sheet_names:
                    s = sheet.strip().lower()
                    if "detail" in s:
                        detail_sheet = sheet
                    if "conversion" in s and "stats" in s:
                        conversion_sheet = sheet

                if detail_sheet is None:
                    st.error("Could not find the Detail sheet in this workbook.")
                    st.info("Please upload the original report workbook that includes a Detail tab.")
                    return

                detail_df = pd.read_excel(uploaded_file, sheet_name=detail_sheet)
                uploaded_file.seek(0)

                if needs_conversion_stats:
                    if conversion_sheet is None:
                        st.warning(
                            "Could not find the Conversion Stats sheet. "
                            "The app will use the report mapping table instead."
                        )
                        conversion_df = pd.DataFrame()
                    else:
                        conversion_df = pd.read_excel(uploaded_file, sheet_name=conversion_sheet)
                        uploaded_file.seek(0)
                        workbook_lookup = build_workbook_lookup(conversion_df)

                id_col = find_column(detail_df, [identifier_label, identifier_label.replace(" ", "")])
                paidamount_col = find_column(detail_df, ["PaidAmount", "Paid Amount"])
                disposition_col = find_column(detail_df, ["Disposition"])

                if id_col is None:
                    st.error(f"Could not find {identifier_label} in the Detail sheet.")
                    st.info("Please verify you uploaded the correct workbook for this report.")
                    return

                merged_df = merge_with_mapping(detail_df, id_col, get_mapping_df(report_key), workbook_lookup)

            else:
                try:
                    detail_df = pd.read_csv(uploaded_file)
                except Exception as e:
                    st.error(f"Could not open this CSV file: {e}")
                    st.info("Please make sure you uploaded a valid CSV export.")
                    return

                id_col = find_column(detail_df, [identifier_label, identifier_label.replace(" ", "")])
                duration_col = find_column(detail_df, ["Duration"])
                disposition_col = find_column(detail_df, ["Disposition"])

                if id_col is None:
                    st.error(f"Could not find {identifier_label} in the CSV.")
                    st.info("Please verify you uploaded the correct ECP CSV report.")
                    return

                if custom_payable_rule == "ecp":
                    if duration_col is None or disposition_col is None:
                        st.error("Could not find Duration and/or Disposition in the ECP CSV.")
                        st.info("ECP requires both Duration and Disposition columns to calculate Payable.")
                        return

                    detail_df["Payable"] = detail_df.apply(
                        lambda row: "Y"
                        if normalize_text(row[disposition_col]).lower() == "transfer sale"
                        and pd.to_numeric(row[duration_col], errors="coerce") >= 120
                        else "N",
                        axis=1
                    )

                merged_df = merge_with_mapping(
                    detail_df,
                    id_col,
                    get_mapping_df(report_key),
                    pd.DataFrame(columns=["Identifier", "CenterName", "Email"])
                )

            # Summary
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

            # Vendor files + zip
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
                        "Identifier_workbook",
                        "Identifier_mapping",
                        "FinalCenterName",
                        "FinalEmail",
                        "CenterName_workbook",
                        "Email_workbook",
                        "CenterName_mapping",
                        "Email_mapping",
                        "Identifier"
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

            # Metrics
            total_centers = len(summary_df)
            total_rows = int(summary_df["TotalRows"].sum()) if not summary_df.empty else 0
            total_payable = int(summary_df["PayableY"].sum()) if not summary_df.empty else 0
            ready_count = int((summary_df["ReadyToSend"] == "Yes").sum()) if not summary_df.empty else 0

            m1, m2, m3, m4 = st.columns(4)
            with m1:
                metric_card("Centers", total_centers)
            with m2:
                metric_card("Total Leads", total_rows)
            with m3:
                metric_card("Payable Leads", total_payable)
            with m4:
                metric_card("Ready to Send", ready_count)

            tab1, tab2, tab3, tab4 = st.tabs(["Review", "Downloads", "Email Queue", "Raw Preview"])

            with tab1:
                st.subheader("Center Summary")
                st.dataframe(summary_df, width="stretch")

                if not missing_email_df.empty:
                    st.warning("Some centers are missing email addresses. Update the mapping page before live sending.")
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
                    use_container_width=True
                )
                st.download_button(
                    label="Download center summary CSV",
                    data=summary_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{report_key}_center_summary.csv",
                    mime="text/csv",
                    use_container_width=True
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

                if st.button("Send Emails", key=f"{report_key}_send_emails", type="primary", use_container_width=True, disabled=send_disabled):
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

                        log_report_run(
                            report_type=report_name,
                            total_rows=total_rows,
                            payable_leads=total_payable,
                            centers=total_centers,
                            emails_sent=sent_count,
                            mode="TEST" if test_mode else "LIVE",
                            missing_emails=len(missing_email_df)
                        )

                    except Exception as e:
                        st.error(f"Email error: {e}")

            with tab4:
                st.subheader("Raw Preview")
                st.dataframe(detail_df.head(20), width="stretch")
                if not conversion_df.empty:
                    st.subheader("Conversion Stats Preview")
                    st.dataframe(conversion_df.head(20), width="stretch")

            st.info(f"Every email in this workflow will CC: {CC_EMAIL}")

        except Exception as e:
            st.error(f"Something went wrong while processing this file: {e}")
            st.info("Please verify that you uploaded the correct report file and that the expected columns are present.")


# =========================
# ROUTER
# =========================
current_page = st.session_state.current_page

if current_page == "dashboard":
    render_dashboard()

elif current_page == "cgm":
    render_report_page(
        report_key="cgm",
        report_name="CGM Report",
        identifier_label="LeadSource",
        file_type="excel",
        needs_conversion_stats=True,
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
        file_type="csv",
        needs_conversion_stats=False,
        remove_columns=[],
        custom_payable_rule="ecp"
    )

elif current_page == "medadv":
    render_report_page(
        report_key="medadv",
        report_name="Med Advantage Report",
        identifier_label="LeadSource",
        file_type="excel",
        needs_conversion_stats=True,
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
