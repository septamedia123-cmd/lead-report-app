import io
import smtplib
import zipfile
import pandas as pd
import streamlit as st
from email.message import EmailMessage

st.set_page_config(
    page_title="LIVMED Lead Portal",
    page_icon="📊",
    layout="wide"
)

CC_EMAIL = "erica@livmed.us"


def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def find_column(df, candidates):
    lower_map = {str(col).strip().lower(): col for col in df.columns}

    for candidate in candidates:
        candidate_lower = candidate.lower()
        if candidate_lower in lower_map:
            return lower_map[candidate_lower]

    for candidate in candidates:
        candidate_lower = candidate.lower()
        for col in df.columns:
            if candidate_lower in str(col).strip().lower():
                return col

    return None


def sanitize_filename(text):
    text = normalize_text(text)
    if not text:
        return "Unknown"
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in text)


def build_email_subject(center_name, identifier):
    center_name = normalize_text(center_name) or "Center"
    identifier = normalize_text(identifier)
    return f"Daily Lead Report - {center_name} - {identifier}"


def build_email_body(center_name, identifier, total_rows, payable_y):
    center_name = normalize_text(center_name) or "Team"
    identifier = normalize_text(identifier)

    return f"""Hello {center_name},

Attached is your daily lead report.

Identifier: {identifier}
Total Records: {total_rows}
Payable Leads: {payable_y}

Please review and let us know if you have any questions.

Best,
Dean
"""


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


def show_header():
    col1, col2 = st.columns([1, 3])

    with col1:
        st.image("./logo.png", width=180)

    with col2:
        st.markdown(
            """
            <div style="padding-top: 20px;">
                <div style="font-size: 34px; font-weight: 800; color: #111827;">
                    LIVMED Lead Report Portal
                </div>
                <div style="font-size: 16px; color: #6b7280; margin-top: 4px;">
                    Secure report processing, file splitting, and vendor delivery system
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<hr style='margin-top: 10px; margin-bottom: 20px;'>", unsafe_allow_html=True)


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


# -----------------------------
# Secure values from Streamlit secrets
# -----------------------------
sender_email = st.secrets["EMAIL"]
gmail_app_password = st.secrets["PASSWORD"]
site_password = st.secrets["APP_PASSWORD"]


# -----------------------------
# Session auth
# -----------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False


# -----------------------------
# Login screen
# -----------------------------
if not st.session_state.authenticated:
    st.markdown(
        """
        <div style="
            max-width: 560px;
            margin: 70px auto 20px auto;
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 20px;
            padding: 34px;
            box-shadow: 0 12px 30px rgba(0,0,0,0.07);
            text-align: center;">
        """,
        unsafe_allow_html=True,
    )

    st.image("./logo.png", width=260)

    st.markdown(
        """
        <div style="font-size: 30px; font-weight: 800; color: #111827; margin-top: 10px;">
            LIVMED Lead Report Portal
        </div>
        <div style="font-size: 15px; color: #6b7280; margin-top: 8px; margin-bottom: 10px;">
            Secure access required
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns([1.2, 2.2, 1.2])

    with col2:
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


# -----------------------------
# Main app
# -----------------------------
show_header()

with st.sidebar:
    st.header("Sending Options")
    test_mode = st.checkbox("Test Mode", value=True)
    test_email = st.text_input("Test Email")
    st.info(f"Every email is automatically CC'd to: {CC_EMAIL}")

    st.divider()
    if st.button("Log out", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()

show_mode_banner(test_mode)

st.write(
    "Upload the daily Excel workbook below. This app uses the **Detail** tab for raw rows and "
    "**Conversion Stats** for center name and email matching."
)

uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx", "xls"])

if uploaded_file:
    try:
        xls = pd.ExcelFile(uploaded_file)
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
            st.error("Could not find the Detail sheet.")
            st.stop()

        if conversion_sheet is None:
            st.error("Could not find the Conversion Stats sheet.")
            st.stop()

        detail_df = pd.read_excel(uploaded_file, sheet_name=detail_sheet)
        uploaded_file.seek(0)
        conversion_df = pd.read_excel(uploaded_file, sheet_name=conversion_sheet)
        uploaded_file.seek(0)

        # Detect columns in Detail
        leadsource_col = find_column(detail_df, ["LeadSource", "Lead Source"])
        payable_col = find_column(detail_df, ["Payable"])
        paidamount_col = find_column(detail_df, ["PaidAmount", "Paid Amount"])
        disposition_col = find_column(detail_df, ["Disposition"])

        if leadsource_col is None:
            st.error("Could not find LeadSource in the Detail sheet.")
            st.stop()

        # Detect columns in Conversion Stats
        identifier_col = conversion_df.columns[0]
        center_name_col = find_column(conversion_df, ["Center Name", "CenterName"])
        email_col = find_column(conversion_df, ["Email", "Email Address"])

        if center_name_col is None:
            st.warning("Could not find Center Name in Conversion Stats.")
        if email_col is None:
            st.warning("Could not find Email in Conversion Stats.")

        conversion_lookup = pd.DataFrame({
            "Identifier": conversion_df[identifier_col].apply(normalize_text),
            "CenterName": conversion_df[center_name_col].apply(normalize_text) if center_name_col else "",
            "Email": conversion_df[email_col].apply(normalize_text) if email_col else "",
        })

        conversion_lookup = conversion_lookup.drop_duplicates(subset=["Identifier"])
        conversion_lookup = conversion_lookup[conversion_lookup["Identifier"] != ""]

        detail_df["LeadSource_normalized"] = detail_df[leadsource_col].apply(normalize_text)

        merged_df = detail_df.merge(
            conversion_lookup,
            left_on="LeadSource_normalized",
            right_on="Identifier",
            how="left"
        )

        # -----------------------------
        # Summary data
        # -----------------------------
        summary_rows = []

        for leadsource, group in merged_df.groupby("LeadSource_normalized"):
            payable_y = 0
            payable_n = 0
            total_paid = 0.0

            if payable_col:
                payable_series = group[payable_col].astype(str).str.strip().str.upper()
                payable_y = int((payable_series == "Y").sum())
                payable_n = int((payable_series == "N").sum())

            if paidamount_col:
                total_paid = float(
                    pd.to_numeric(group[paidamount_col], errors="coerce").fillna(0).sum()
                )

            center_name = normalize_text(group["CenterName"].iloc[0]) if "CenterName" in group.columns else ""
            email = normalize_text(group["Email"].iloc[0]) if "Email" in group.columns else ""

            summary_rows.append({
                "Identifier": leadsource,
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

        if disposition_col:
            disposition_summary = (
                merged_df.groupby(["LeadSource_normalized", disposition_col])
                .size()
                .reset_index(name="Count")
                .rename(columns={"LeadSource_normalized": "Identifier"})
            )
        else:
            disposition_summary = pd.DataFrame()

        missing_email_df = pd.DataFrame()
        if not summary_df.empty:
            missing_email_df = summary_df[summary_df["Email"].astype(str).str.strip() == ""]

        # -----------------------------
        # Build vendor files
        # -----------------------------
        zip_buffer = io.BytesIO()
        vendor_files = []

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for leadsource, group in merged_df.groupby("LeadSource_normalized"):
                center_name = normalize_text(group["CenterName"].iloc[0]) if "CenterName" in group.columns else ""
                email = normalize_text(group["Email"].iloc[0]) if "Email" in group.columns else ""

                safe_identifier = sanitize_filename(leadsource)
                safe_center = sanitize_filename(center_name) if center_name else "UnknownCenter"

                columns_to_remove = [
                    "LeadSource_normalized",
                    "Identifier",
                    "CenterName",
                    "Email",
                    "PaidAmount",
                    "Paid Amount",
                    "DiabeticOnMedicare",
                    "Diabetic On Medicare",
                    "AssignedTo",
                    "Assigned To"
                ]

                export_group = group.drop(columns=columns_to_remove, errors="ignore")

                csv_bytes = export_group.to_csv(index=False).encode("utf-8")
                csv_filename = f"{safe_center}__{safe_identifier}.csv"
                zip_file.writestr(csv_filename, csv_bytes)

                payable_y = 0
                if payable_col:
                    payable_series = group[payable_col].astype(str).str.strip().str.upper()
                    payable_y = int((payable_series == "Y").sum())

                vendor_files.append({
                    "Identifier": leadsource,
                    "CenterName": center_name,
                    "Email": email,
                    "CC": CC_EMAIL,
                    "FileName": csv_filename,
                    "CSVBytes": csv_bytes,
                    "TotalRows": len(group),
                    "PayableY": payable_y,
                })

            zip_file.writestr("center_summary.csv", summary_df.to_csv(index=False).encode("utf-8"))

            if not disposition_summary.empty:
                zip_file.writestr("disposition_summary.csv", disposition_summary.to_csv(index=False).encode("utf-8"))

        zip_buffer.seek(0)

        # -----------------------------
        # Top metrics
        # -----------------------------
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

        # -----------------------------
        # Tabs
        # -----------------------------
        tab1, tab2, tab3, tab4 = st.tabs(["Review", "Downloads", "Email Queue", "Raw Preview"])

        with tab1:
            st.subheader("Center Summary")
            st.dataframe(summary_df, width="stretch")

            if not missing_email_df.empty:
                st.warning("Some centers are missing email addresses. These should not be emailed until fixed.")
                st.dataframe(missing_email_df, width="stretch")

            if not disposition_summary.empty:
                st.subheader("Disposition Summary")
                st.dataframe(disposition_summary, width="stretch")

        with tab2:
            st.subheader("Downloads")
            st.download_button(
                label="Download ZIP of all center CSV files",
                data=zip_buffer,
                file_name="split_center_reports.zip",
                mime="application/zip",
                use_container_width=True
            )

            st.download_button(
                label="Download center summary CSV",
                data=summary_df.to_csv(index=False).encode("utf-8"),
                file_name="center_summary.csv",
                mime="text/csv",
                use_container_width=True
            )

            st.caption(
                "Vendor-facing CSV exports automatically remove PaidAmount, DiabeticOnMedicare, "
                "AssignedTo, and internal matching columns."
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
                live_confirm = st.checkbox("I confirm I want to send emails to live vendor addresses.")

            send_disabled = False
            if test_mode and not test_email.strip():
                send_disabled = True
                st.info("Enter a test email in the sidebar to enable sending in Test Mode.")

            if not test_mode and not live_confirm:
                send_disabled = True
                st.warning("Live sending requires confirmation before emails can be sent.")

            if st.button("Send Emails", type="primary", use_container_width=True, disabled=send_disabled):
                try:
                    server = smtplib.SMTP("smtp.gmail.com", 587)
                    server.starttls()
                    server.login(sender_email, gmail_app_password)

                    sent_count = 0

                    for item in vendor_files:
                        if not test_mode and not item["Email"]:
                            continue

                        to_email = test_email.strip() if test_mode else item["Email"]

                        msg = EmailMessage()
                        msg["Subject"] = build_email_subject(item["CenterName"], item["Identifier"])
                        msg["From"] = sender_email
                        msg["To"] = to_email
                        msg["CC"] = item["CC"]
                        msg.set_content(
                            build_email_body(
                                item["CenterName"],
                                item["Identifier"],
                                item["TotalRows"],
                                item["PayableY"]
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
                    st.success(f"Sent {sent_count} emails successfully.")

                except Exception as e:
                    st.error(f"Email error: {e}")

        with tab4:
            st.subheader("Preview of Detail Sheet")
            st.dataframe(detail_df.head(10), width="stretch")

            st.subheader("Preview of Conversion Stats")
            st.dataframe(conversion_df.head(10), width="stretch")

        st.info(f"Every email will CC: {CC_EMAIL}")

    except Exception as e:
        st.error(f"Something went wrong: {e}")

else:
    st.info("Upload an Excel workbook to begin.")
