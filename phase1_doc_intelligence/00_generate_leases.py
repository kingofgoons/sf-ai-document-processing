#!/usr/bin/env python3
"""
Generate synthetic industrial lease PDFs for AI/ML demo.
Creates leases with varying risk levels for AI_CLASSIFY demonstration.
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import random
import os

# Output directory
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "leases")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Sample data for realistic lease generation
MARKETS = ["Chicago", "Dallas", "Atlanta", "Los Angeles", "New Jersey", "Phoenix", "Denver"]
TENANT_COMPANIES = [
    ("Apex Distribution LLC", "Delaware"),
    ("GlobalTech Fulfillment Inc.", "California"),
    ("Midwest Logistics Partners", "Illinois"),
    ("SunBelt Supply Chain Co.", "Texas"),
    ("Pacific Freight Solutions", "Nevada"),
    ("Eastern Seaboard Warehousing", "New Jersey"),
]
PROPERTY_ADDRESSES = [
    ("1500 Industrial Parkway", "Romeoville", "IL", "60446"),
    ("8200 Commerce Drive", "Irving", "TX", "75063"),
    ("3400 Logistics Boulevard", "McDonough", "GA", "30253"),
    ("12000 Distribution Way", "Ontario", "CA", "91761"),
    ("500 Terminal Road", "Elizabeth", "NJ", "07201"),
    ("7800 Warehouse Lane", "Phoenix", "AZ", "85043"),
]


def generate_lease_id():
    """Generate a realistic lease ID."""
    return f"LNK-{random.randint(2023, 2025)}-{random.randint(10000, 99999)}"


def get_styles():
    """Create custom styles for the lease document."""
    styles = getSampleStyleSheet()
    
    styles.add(ParagraphStyle(
        name='LeaseTitle',
        parent=styles['Heading1'],
        fontSize=16,
        alignment=TA_CENTER,
        spaceAfter=20,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Heading2'],
        fontSize=12,
        spaceBefore=15,
        spaceAfter=8,
        fontName='Helvetica-Bold'
    ))
    
    styles.add(ParagraphStyle(
        name='LeaseBody',
        parent=styles['Normal'],
        fontSize=10,
        alignment=TA_JUSTIFY,
        spaceAfter=8,
        leading=14
    ))
    
    styles.add(ParagraphStyle(
        name='LeaseClause',
        parent=styles['Normal'],
        fontSize=10,
        leftIndent=20,
        spaceAfter=6,
        leading=13
    ))
    
    return styles


def generate_high_risk_lease(lease_num: int):
    """Generate a high-risk lease with unusual terms requiring legal review."""
    
    lease_id = generate_lease_id()
    tenant, tenant_state = random.choice(TENANT_COMPANIES)
    address, city, state, zip_code = random.choice(PROPERTY_ADDRESSES)
    market = random.choice(MARKETS)
    
    start_date = datetime.now() + timedelta(days=random.randint(30, 90))
    term_months = random.choice([36, 60, 84])
    end_date = start_date + relativedelta(months=term_months)
    
    sqft = random.randint(150000, 400000)
    rent_psf = round(random.uniform(4.50, 7.50), 2)
    annual_rent = sqft * rent_psf
    
    styles = get_styles()
    
    filename = f"lease_high_risk_{lease_num:03d}.pdf"
    filepath = os.path.join(OUTPUT_DIR, filename)
    doc = SimpleDocTemplate(filepath, pagesize=letter, 
                           leftMargin=0.75*inch, rightMargin=0.75*inch,
                           topMargin=0.75*inch, bottomMargin=0.75*inch)
    
    story = []
    
    # Title
    story.append(Paragraph("INDUSTRIAL LEASE AGREEMENT", styles['LeaseTitle']))
    story.append(Paragraph(f"<b>Lease ID:</b> {lease_id}", styles['LeaseBody']))
    story.append(Spacer(1, 0.2*inch))
    
    # Parties
    story.append(Paragraph("ARTICLE 1: PARTIES", styles['SectionHeader']))
    story.append(Paragraph(
        f"<b>LANDLORD:</b> Acme Industrial Real Estate, LLC, a Delaware limited liability company",
        styles['LeaseBody']))
    story.append(Paragraph(
        f"<b>TENANT:</b> {tenant}, a {tenant_state} limited liability company",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    # Premises
    story.append(Paragraph("ARTICLE 2: PREMISES", styles['SectionHeader']))
    story.append(Paragraph(
        f"Landlord hereby leases to Tenant the following described premises (the \"Premises\"):",
        styles['LeaseBody']))
    story.append(Paragraph(f"<b>Property Address:</b> {address}, {city}, {state} {zip_code}", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Market:</b> {market}", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Rentable Square Footage:</b> {sqft:,} RSF", styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Lease Term
    story.append(Paragraph("ARTICLE 3: LEASE TERM", styles['SectionHeader']))
    story.append(Paragraph(
        f"<b>Commencement Date:</b> {start_date.strftime('%B %d, %Y')}",
        styles['LeaseClause']))
    story.append(Paragraph(
        f"<b>Expiration Date:</b> {end_date.strftime('%B %d, %Y')}",
        styles['LeaseClause']))
    story.append(Paragraph(
        f"<b>Lease Term:</b> {term_months} months",
        styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Financial Terms
    story.append(Paragraph("ARTICLE 4: FINANCIAL TERMS", styles['SectionHeader']))
    story.append(Paragraph(f"<b>Base Rent:</b> ${annual_rent:,.2f} annually (${rent_psf:.2f} per RSF)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Rent Escalation:</b> 1% annually (BELOW MARKET)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Security Deposit:</b> NONE REQUIRED", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Free Rent Period:</b> 12 months", styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # HIGH RISK CLAUSES
    story.append(Paragraph("ARTICLE 5: MAINTENANCE AND REPAIRS", styles['SectionHeader']))
    story.append(Paragraph(
        "5.1 <b>Landlord Responsibilities (EXPANDED):</b> Landlord shall be responsible for ALL repairs "
        "and maintenance of the Premises, including but not limited to: roof, structure, foundation, "
        "exterior walls, HVAC systems, plumbing, electrical systems, parking lot, landscaping, "
        "AND all interior cosmetic repairs and replacements. Tenant shall have no maintenance obligations whatsoever.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 6: ENVIRONMENTAL LIABILITY", styles['SectionHeader']))
    story.append(Paragraph(
        "6.1 <b>Unlimited Landlord Liability:</b> Landlord shall be solely responsible for any and all "
        "environmental contamination discovered on the Premises, whether pre-existing or arising during "
        "the Lease Term, regardless of the source or cause of such contamination. Landlord agrees to "
        "indemnify, defend, and hold Tenant harmless from any environmental claims, with NO CAP on liability.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 7: TERMINATION RIGHTS", styles['SectionHeader']))
    story.append(Paragraph(
        "7.1 <b>Tenant Early Termination (UNUSUAL):</b> Tenant may terminate this Lease at any time "
        "upon thirty (30) days' written notice to Landlord, WITHOUT PENALTY and without payment of "
        "any termination fee or remaining rent obligations. Upon such termination, Tenant shall have "
        "no further obligations under this Lease.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 8: SUBLETTING AND ASSIGNMENT", styles['SectionHeader']))
    story.append(Paragraph(
        "8.1 <b>Unrestricted Subletting:</b> Tenant shall have the absolute right to sublet all or any "
        "portion of the Premises, or assign this Lease, WITHOUT Landlord's consent and without any "
        "requirement to share sublease profits with Landlord.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 9: TENANT IMPROVEMENTS", styles['SectionHeader']))
    story.append(Paragraph(
        f"9.1 <b>Tenant Improvement Allowance (UNCAPPED):</b> Landlord shall provide Tenant with a "
        f"Tenant Improvement Allowance with NO MAXIMUM CAP. Landlord shall reimburse Tenant for all "
        f"costs of improvements, alterations, and build-out of the Premises as requested by Tenant.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 10: RENT ADJUSTMENT", styles['SectionHeader']))
    story.append(Paragraph(
        "10.1 <b>Revenue-Based Rent Reduction:</b> In the event Tenant's gross revenues from operations "
        "at the Premises decline by more than 10% in any calendar year, Base Rent shall be automatically "
        "reduced by a percentage equal to such revenue decline, with no floor or minimum rent.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 11: EXCLUSIVE USE", styles['SectionHeader']))
    story.append(Paragraph(
        "11.1 <b>Portfolio-Wide Exclusive Use:</b> Landlord covenants that it shall not lease any space "
        "in any property owned or managed by Landlord or its affiliates within a 25-mile radius to any "
        "tenant engaged in the same or similar business as Tenant.",
        styles['LeaseBody']))
    
    # Build PDF
    doc.build(story)
    print(f"Generated HIGH RISK lease: {filename}")
    return filename


def generate_medium_risk_lease(lease_num: int):
    """Generate a medium-risk lease with terms that warrant review."""
    
    lease_id = generate_lease_id()
    tenant, tenant_state = random.choice(TENANT_COMPANIES)
    address, city, state, zip_code = random.choice(PROPERTY_ADDRESSES)
    market = random.choice(MARKETS)
    
    start_date = datetime.now() + timedelta(days=random.randint(30, 90))
    term_months = random.choice([60, 84, 120])
    end_date = start_date + relativedelta(months=term_months)
    
    sqft = random.randint(100000, 300000)
    rent_psf = round(random.uniform(5.00, 8.00), 2)
    annual_rent = sqft * rent_psf
    ti_allowance = 55  # Above market
    security_deposit = annual_rent / 12  # Only 1 month
    
    styles = get_styles()
    
    filename = f"lease_medium_risk_{lease_num:03d}.pdf"
    filepath = os.path.join(OUTPUT_DIR, filename)
    doc = SimpleDocTemplate(filepath, pagesize=letter,
                           leftMargin=0.75*inch, rightMargin=0.75*inch,
                           topMargin=0.75*inch, bottomMargin=0.75*inch)
    
    story = []
    
    # Title
    story.append(Paragraph("INDUSTRIAL LEASE AGREEMENT", styles['LeaseTitle']))
    story.append(Paragraph(f"<b>Lease ID:</b> {lease_id}", styles['LeaseBody']))
    story.append(Spacer(1, 0.2*inch))
    
    # Parties
    story.append(Paragraph("ARTICLE 1: PARTIES", styles['SectionHeader']))
    story.append(Paragraph(
        f"<b>LANDLORD:</b> Acme Industrial Real Estate, LLC, a Delaware limited liability company",
        styles['LeaseBody']))
    story.append(Paragraph(
        f"<b>TENANT:</b> {tenant}, a {tenant_state} corporation",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    # Premises
    story.append(Paragraph("ARTICLE 2: PREMISES", styles['SectionHeader']))
    story.append(Paragraph(
        f"Landlord hereby leases to Tenant the following described premises (the \"Premises\"):",
        styles['LeaseBody']))
    story.append(Paragraph(f"<b>Property Address:</b> {address}, {city}, {state} {zip_code}", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Market:</b> {market}", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Rentable Square Footage:</b> {sqft:,} RSF", styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Lease Term
    story.append(Paragraph("ARTICLE 3: LEASE TERM", styles['SectionHeader']))
    story.append(Paragraph(
        f"<b>Commencement Date:</b> {start_date.strftime('%B %d, %Y')}",
        styles['LeaseClause']))
    story.append(Paragraph(
        f"<b>Expiration Date:</b> {end_date.strftime('%B %d, %Y')}",
        styles['LeaseClause']))
    story.append(Paragraph(
        f"<b>Lease Term:</b> {term_months} months",
        styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Financial Terms - Medium Risk indicators
    story.append(Paragraph("ARTICLE 4: FINANCIAL TERMS", styles['SectionHeader']))
    story.append(Paragraph(f"<b>Base Rent:</b> ${annual_rent:,.2f} annually (${rent_psf:.2f} per RSF)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Rent Escalation:</b> 1.5% annually", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Security Deposit:</b> ${security_deposit:,.2f} (1 month's rent)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Free Rent Period:</b> 8 months", styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Medium Risk Clauses
    story.append(Paragraph("ARTICLE 5: TENANT IMPROVEMENTS", styles['SectionHeader']))
    story.append(Paragraph(
        f"5.1 <b>Tenant Improvement Allowance:</b> Landlord shall provide Tenant with a Tenant "
        f"Improvement Allowance of ${ti_allowance:.2f} per rentable square foot "
        f"(${ti_allowance * sqft:,.2f} total), which is above current market rates.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 6: OPERATING EXPENSES", styles['SectionHeader']))
    story.append(Paragraph(
        "6.1 <b>CAM Cap:</b> Notwithstanding any provision to the contrary, Tenant's share of "
        "Common Area Maintenance (CAM) charges shall not increase by more than 3% annually, "
        "regardless of actual increases in operating expenses.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 7: EARLY TERMINATION", styles['SectionHeader']))
    story.append(Paragraph(
        "7.1 <b>Termination Option:</b> Tenant shall have the one-time right to terminate this Lease "
        "effective at the end of the 36th month of the Lease Term, upon six (6) months' prior written "
        "notice and payment of a termination fee equal to three (3) months' Base Rent.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 8: EXPANSION AND REFUSAL RIGHTS", styles['SectionHeader']))
    story.append(Paragraph(
        "8.1 <b>Right of First Refusal:</b> Tenant shall have a Right of First Refusal on any adjacent "
        "space that becomes available during the Lease Term. Landlord must notify Tenant of available "
        "space and Tenant shall have 15 business days to exercise this right.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 9: CO-TENANCY", styles['SectionHeader']))
    story.append(Paragraph(
        "9.1 <b>Co-Tenancy Clause:</b> In the event that the anchor tenant in the industrial park "
        "vacates or ceases operations, Tenant's Base Rent shall be reduced by 25% until such space "
        "is re-leased to a replacement tenant of comparable quality.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 10: MAINTENANCE", styles['SectionHeader']))
    story.append(Paragraph(
        "10.1 <b>Landlord Responsibilities:</b> Landlord shall be responsible for roof, structure, "
        "foundation, and exterior walls. Tenant shall be responsible for HVAC systems, but Landlord "
        "shall be responsible for HVAC replacement if system failure occurs within the first 5 years.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 11: AUDIT RIGHTS", styles['SectionHeader']))
    story.append(Paragraph(
        "11.1 <b>Limited Audit Rights:</b> Landlord's right to audit Tenant's books and records "
        "shall be limited to once per calendar year, with 30 days' prior written notice, and such "
        "audit shall be conducted at Landlord's sole expense.",
        styles['LeaseBody']))
    
    # Build PDF
    doc.build(story)
    print(f"Generated MEDIUM RISK lease: {filename}")
    return filename


def generate_low_risk_lease(lease_num: int):
    """Generate a low-risk lease with standard market terms."""
    
    lease_id = generate_lease_id()
    tenant, tenant_state = random.choice(TENANT_COMPANIES)
    address, city, state, zip_code = random.choice(PROPERTY_ADDRESSES)
    market = random.choice(MARKETS)
    
    start_date = datetime.now() + timedelta(days=random.randint(30, 90))
    term_months = random.choice([60, 84, 120])
    end_date = start_date + relativedelta(months=term_months)
    
    sqft = random.randint(75000, 250000)
    rent_psf = round(random.uniform(5.50, 8.50), 2)
    annual_rent = sqft * rent_psf
    monthly_rent = annual_rent / 12
    security_deposit = monthly_rent * 2  # Standard 2 months
    ti_allowance = 25  # Market rate
    cam_psf = round(random.uniform(1.50, 2.50), 2)
    
    styles = get_styles()
    
    filename = f"lease_standard_{lease_num:03d}.pdf"
    filepath = os.path.join(OUTPUT_DIR, filename)
    doc = SimpleDocTemplate(filepath, pagesize=letter,
                           leftMargin=0.75*inch, rightMargin=0.75*inch,
                           topMargin=0.75*inch, bottomMargin=0.75*inch)
    
    story = []
    
    # Title
    story.append(Paragraph("INDUSTRIAL LEASE AGREEMENT", styles['LeaseTitle']))
    story.append(Paragraph(f"<b>Lease ID:</b> {lease_id}", styles['LeaseBody']))
    story.append(Spacer(1, 0.2*inch))
    
    # Parties
    story.append(Paragraph("ARTICLE 1: PARTIES", styles['SectionHeader']))
    story.append(Paragraph(
        f"<b>LANDLORD:</b> Acme Industrial Real Estate, LLC, a Delaware limited liability company",
        styles['LeaseBody']))
    story.append(Paragraph(
        f"<b>TENANT:</b> {tenant}, a {tenant_state} corporation",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    # Premises
    story.append(Paragraph("ARTICLE 2: PREMISES", styles['SectionHeader']))
    story.append(Paragraph(
        f"Landlord hereby leases to Tenant the following described premises (the \"Premises\"):",
        styles['LeaseBody']))
    story.append(Paragraph(f"<b>Property Address:</b> {address}, {city}, {state} {zip_code}", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Market:</b> {market}", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Rentable Square Footage:</b> {sqft:,} RSF", styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Lease Term
    story.append(Paragraph("ARTICLE 3: LEASE TERM", styles['SectionHeader']))
    story.append(Paragraph(
        f"<b>Commencement Date:</b> {start_date.strftime('%B %d, %Y')}",
        styles['LeaseClause']))
    story.append(Paragraph(
        f"<b>Expiration Date:</b> {end_date.strftime('%B %d, %Y')}",
        styles['LeaseClause']))
    story.append(Paragraph(
        f"<b>Lease Term:</b> {term_months} months",
        styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Financial Terms - Standard/Market
    story.append(Paragraph("ARTICLE 4: FINANCIAL TERMS", styles['SectionHeader']))
    story.append(Paragraph(f"<b>Base Rent:</b> ${annual_rent:,.2f} annually (${rent_psf:.2f} per RSF)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Rent Escalation:</b> 3% annually", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Security Deposit:</b> ${security_deposit:,.2f} (2 months' rent)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>CAM Charges:</b> ${cam_psf:.2f} per RSF annually (estimated)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Lease Type:</b> Triple Net (NNN)", styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Standard Clauses
    story.append(Paragraph("ARTICLE 5: TRIPLE NET OBLIGATIONS", styles['SectionHeader']))
    story.append(Paragraph(
        "5.1 <b>Tenant Obligations:</b> This is a Triple Net (NNN) Lease. In addition to Base Rent, "
        "Tenant shall pay its proportionate share of: (a) real estate taxes; (b) property insurance; "
        "and (c) common area maintenance expenses. Tenant shall also maintain and insure the Premises.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 6: TENANT IMPROVEMENTS", styles['SectionHeader']))
    story.append(Paragraph(
        f"6.1 <b>Tenant Improvement Allowance:</b> Landlord shall provide Tenant with a Tenant "
        f"Improvement Allowance of ${ti_allowance:.2f} per rentable square foot "
        f"(${ti_allowance * sqft:,.2f} total). Any improvements exceeding this allowance shall be "
        f"at Tenant's sole cost and expense.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 7: MAINTENANCE AND REPAIRS", styles['SectionHeader']))
    story.append(Paragraph(
        "7.1 <b>Landlord Responsibilities:</b> Landlord shall maintain and repair the roof, "
        "structural components, foundation, and exterior walls of the building.",
        styles['LeaseBody']))
    story.append(Paragraph(
        "7.2 <b>Tenant Responsibilities:</b> Tenant shall maintain and repair all other portions "
        "of the Premises, including HVAC systems, plumbing, electrical, interior walls, flooring, "
        "and all Tenant improvements.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 8: INSURANCE", styles['SectionHeader']))
    story.append(Paragraph(
        "8.1 <b>Required Coverage:</b> Tenant shall maintain commercial general liability insurance "
        "with limits of not less than $1,000,000 per occurrence and $2,000,000 aggregate, naming "
        "Landlord as additional insured. Tenant shall also maintain property insurance covering "
        "Tenant's personal property and improvements.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 9: ASSIGNMENT AND SUBLETTING", styles['SectionHeader']))
    story.append(Paragraph(
        "9.1 <b>Consent Required:</b> Tenant shall not assign this Lease or sublet all or any portion "
        "of the Premises without Landlord's prior written consent, which consent shall not be "
        "unreasonably withheld, conditioned, or delayed.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 10: DEFAULT AND REMEDIES", styles['SectionHeader']))
    story.append(Paragraph(
        "10.1 <b>Notice and Cure:</b> In the event of any default by Tenant, Landlord shall provide "
        "written notice specifying the default. Tenant shall have thirty (30) days to cure any "
        "monetary default and thirty (30) days to cure any non-monetary default (or such longer "
        "period as reasonably necessary if cure cannot be completed within 30 days).",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 11: PERSONAL GUARANTEE", styles['SectionHeader']))
    story.append(Paragraph(
        "11.1 <b>Guarantee:</b> The principal(s) of Tenant shall execute a personal guarantee of "
        "Tenant's obligations under this Lease, in the form attached hereto as Exhibit C.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTICLE 12: TERMINATION NOTICE", styles['SectionHeader']))
    story.append(Paragraph(
        "12.1 <b>Required Notice:</b> Either party must provide at least sixty (60) days' prior "
        "written notice of any intent to terminate this Lease at the expiration of the Lease Term "
        "or any renewal period.",
        styles['LeaseBody']))
    
    # Build PDF
    doc.build(story)
    print(f"Generated LOW RISK (standard) lease: {filename}")
    return filename


def generate_spanish_lease_miami():
    """Generate a Spanish-language lease for a Miami tenant (Low Risk, standard terms)."""
    
    lease_id = "LNK-2024-78543"
    tenant = "Distribuidora Caribe, S.A."
    tenant_state = "Florida"
    address = "9500 NW 112th Avenue"
    city = "Miami"
    state = "FL"
    zip_code = "33178"
    market = "Miami"
    
    start_date = datetime.now() + timedelta(days=45)
    term_months = 60
    end_date = start_date + relativedelta(months=term_months)
    
    sqft = 185000
    rent_psf = 7.25
    annual_rent = sqft * rent_psf
    monthly_rent = annual_rent / 12
    security_deposit = monthly_rent * 2
    ti_allowance = 25
    cam_psf = 2.10
    
    styles = get_styles()
    
    filename = "lease_spanish_miami_001.pdf"
    filepath = os.path.join(OUTPUT_DIR, filename)
    doc = SimpleDocTemplate(filepath, pagesize=letter,
                           leftMargin=0.75*inch, rightMargin=0.75*inch,
                           topMargin=0.75*inch, bottomMargin=0.75*inch)
    
    story = []
    
    # Title - Spanish
    story.append(Paragraph("CONTRATO DE ARRENDAMIENTO INDUSTRIAL", styles['LeaseTitle']))
    story.append(Paragraph(f"<b>Número de Contrato:</b> {lease_id}", styles['LeaseBody']))
    story.append(Spacer(1, 0.2*inch))
    
    # Parties - Spanish
    story.append(Paragraph("ARTÍCULO 1: PARTES CONTRATANTES", styles['SectionHeader']))
    story.append(Paragraph(
        f"<b>ARRENDADOR:</b> Acme Industrial Real Estate, LLC, una compañía de responsabilidad limitada de Delaware",
        styles['LeaseBody']))
    story.append(Paragraph(
        f"<b>ARRENDATARIO:</b> {tenant}, una corporación de {tenant_state}",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    # Premises - Spanish
    story.append(Paragraph("ARTÍCULO 2: INMUEBLE ARRENDADO", styles['SectionHeader']))
    story.append(Paragraph(
        f"El Arrendador por medio del presente arrienda al Arrendatario el siguiente inmueble descrito (el \"Inmueble\"):",
        styles['LeaseBody']))
    story.append(Paragraph(f"<b>Dirección del Inmueble:</b> {address}, {city}, {state} {zip_code}", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Mercado:</b> {market}", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Pies Cuadrados Rentables:</b> {sqft:,} PSC", styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Lease Term - Spanish
    story.append(Paragraph("ARTÍCULO 3: PLAZO DEL ARRENDAMIENTO", styles['SectionHeader']))
    story.append(Paragraph(
        f"<b>Fecha de Inicio:</b> {start_date.strftime('%d de %B de %Y').replace('January', 'enero').replace('February', 'febrero').replace('March', 'marzo').replace('April', 'abril').replace('May', 'mayo').replace('June', 'junio').replace('July', 'julio').replace('August', 'agosto').replace('September', 'septiembre').replace('October', 'octubre').replace('November', 'noviembre').replace('December', 'diciembre')}",
        styles['LeaseClause']))
    story.append(Paragraph(
        f"<b>Fecha de Vencimiento:</b> {end_date.strftime('%d de %B de %Y').replace('January', 'enero').replace('February', 'febrero').replace('March', 'marzo').replace('April', 'abril').replace('May', 'mayo').replace('June', 'junio').replace('July', 'julio').replace('August', 'agosto').replace('September', 'septiembre').replace('October', 'octubre').replace('November', 'noviembre').replace('December', 'diciembre')}",
        styles['LeaseClause']))
    story.append(Paragraph(
        f"<b>Plazo del Arrendamiento:</b> {term_months} meses",
        styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Financial Terms - Spanish (Standard/Low Risk)
    story.append(Paragraph("ARTÍCULO 4: TÉRMINOS FINANCIEROS", styles['SectionHeader']))
    story.append(Paragraph(f"<b>Renta Base:</b> ${annual_rent:,.2f} anuales (${rent_psf:.2f} por PSC)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Incremento de Renta:</b> 3% anual", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Depósito de Seguridad:</b> ${security_deposit:,.2f} (equivalente a 2 meses de renta)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Gastos de Áreas Comunes:</b> ${cam_psf:.2f} por PSC anualmente (estimado)", styles['LeaseClause']))
    story.append(Paragraph(f"<b>Tipo de Arrendamiento:</b> Triple Neto (NNN)", styles['LeaseClause']))
    story.append(Spacer(1, 0.1*inch))
    
    # Standard Clauses - Spanish
    story.append(Paragraph("ARTÍCULO 5: OBLIGACIONES TRIPLE NETO", styles['SectionHeader']))
    story.append(Paragraph(
        "5.1 <b>Obligaciones del Arrendatario:</b> Este es un Arrendamiento Triple Neto (NNN). Además de la Renta Base, "
        "el Arrendatario pagará su parte proporcional de: (a) impuestos inmobiliarios; (b) seguro del inmueble; "
        "y (c) gastos de mantenimiento de áreas comunes. El Arrendatario también mantendrá y asegurará el Inmueble.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTÍCULO 6: MEJORAS DEL ARRENDATARIO", styles['SectionHeader']))
    story.append(Paragraph(
        f"6.1 <b>Asignación para Mejoras:</b> El Arrendador proporcionará al Arrendatario una Asignación para Mejoras "
        f"de ${ti_allowance:.2f} por pie cuadrado rentable (${ti_allowance * sqft:,.2f} total). Cualquier mejora "
        f"que exceda esta asignación será por cuenta y cargo exclusivo del Arrendatario.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTÍCULO 7: MANTENIMIENTO Y REPARACIONES", styles['SectionHeader']))
    story.append(Paragraph(
        "7.1 <b>Responsabilidades del Arrendador:</b> El Arrendador mantendrá y reparará el techo, "
        "componentes estructurales, cimientos y paredes exteriores del edificio.",
        styles['LeaseBody']))
    story.append(Paragraph(
        "7.2 <b>Responsabilidades del Arrendatario:</b> El Arrendatario mantendrá y reparará todas las demás "
        "partes del Inmueble, incluyendo sistemas de HVAC, plomería, electricidad, paredes interiores, pisos "
        "y todas las mejoras realizadas por el Arrendatario.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTÍCULO 8: SEGUROS", styles['SectionHeader']))
    story.append(Paragraph(
        "8.1 <b>Cobertura Requerida:</b> El Arrendatario mantendrá un seguro de responsabilidad civil general "
        "comercial con límites no menores a $1,000,000 por ocurrencia y $2,000,000 en total, nombrando "
        "al Arrendador como asegurado adicional. El Arrendatario también mantendrá seguro de propiedad "
        "cubriendo los bienes personales y mejoras del Arrendatario.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTÍCULO 9: CESIÓN Y SUBARRENDAMIENTO", styles['SectionHeader']))
    story.append(Paragraph(
        "9.1 <b>Consentimiento Requerido:</b> El Arrendatario no cederá este Contrato ni subarrendará todo o "
        "parte del Inmueble sin el consentimiento previo por escrito del Arrendador, el cual no será "
        "denegado, condicionado o retrasado injustificadamente.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTÍCULO 10: INCUMPLIMIENTO Y REMEDIOS", styles['SectionHeader']))
    story.append(Paragraph(
        "10.1 <b>Notificación y Subsanación:</b> En caso de cualquier incumplimiento por parte del Arrendatario, "
        "el Arrendador proporcionará notificación por escrito especificando el incumplimiento. El Arrendatario "
        "tendrá treinta (30) días para subsanar cualquier incumplimiento monetario y treinta (30) días para "
        "subsanar cualquier incumplimiento no monetario.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTÍCULO 11: GARANTÍA PERSONAL", styles['SectionHeader']))
    story.append(Paragraph(
        "11.1 <b>Garantía:</b> Los principales del Arrendatario ejecutarán una garantía personal de las "
        "obligaciones del Arrendatario bajo este Contrato, en la forma adjunta como Anexo C.",
        styles['LeaseBody']))
    story.append(Spacer(1, 0.1*inch))
    
    story.append(Paragraph("ARTÍCULO 12: AVISO DE TERMINACIÓN", styles['SectionHeader']))
    story.append(Paragraph(
        "12.1 <b>Aviso Requerido:</b> Cualquiera de las partes debe proporcionar al menos sesenta (60) días "
        "de aviso previo por escrito de cualquier intención de terminar este Contrato al vencimiento del "
        "Plazo del Arrendamiento o cualquier período de renovación.",
        styles['LeaseBody']))
    
    # Build PDF
    doc.build(story)
    print(f"Generated SPANISH (Miami) lease: {filename}")
    return filename


def main():
    """Generate all synthetic lease documents."""
    print("=" * 60)
    print("Generating Synthetic Industrial Lease Documents")
    print("=" * 60)
    print()
    
    generated_files = []
    
    # Generate High Risk leases (2)
    print("Generating HIGH RISK leases...")
    for i in range(1, 3):
        generated_files.append(generate_high_risk_lease(i))
    print()
    
    # Generate Medium Risk leases (2)
    print("Generating MEDIUM RISK leases...")
    for i in range(1, 3):
        generated_files.append(generate_medium_risk_lease(i))
    print()
    
    # Generate Low Risk (Standard) leases (2)
    print("Generating LOW RISK (standard) leases...")
    for i in range(1, 3):
        generated_files.append(generate_low_risk_lease(i))
    print()
    
    # Generate Spanish lease for Miami tenant (1)
    print("Generating SPANISH (Miami) lease...")
    generated_files.append(generate_spanish_lease_miami())
    print()
    
    print("=" * 60)
    print(f"Generated {len(generated_files)} lease documents in: {OUTPUT_DIR}")
    print("=" * 60)
    for f in generated_files:
        print(f"  - {f}")
    
    return generated_files


if __name__ == "__main__":
    main()
