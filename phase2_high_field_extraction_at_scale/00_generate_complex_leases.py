#!/usr/bin/env python3
"""
Generate complex synthetic industrial lease PDFs with 351 extractable fields.

These leases are designed to exceed AI_EXTRACT's 100-question-per-call limit,
demonstrating the need for multi-pass extraction or AI_COMPLETE structured output.

Fields are organized into 9 sections:
  1. Core Terms          (~30 fields)
  2. Financial           (~50 fields)
  3. Operating Expenses  (~40 fields)
  4. Options             (~45 fields)
  5. Insurance & Liability (~40 fields)
  6. Construction & Alterations (~35 fields)
  7. Default & Remedies  (~40 fields)
  8. Environmental & Compliance (~35 fields)
  9. Miscellaneous       (~36 fields)
                         ≈ 351 total
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
import string

# Output directory
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "leases")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Sample data pools
# ---------------------------------------------------------------------------
MARKETS = [
    "Chicago", "Dallas-Fort Worth", "Atlanta", "Inland Empire",
    "Northern New Jersey", "Phoenix", "Denver", "Houston",
    "Memphis", "Indianapolis", "Columbus", "Nashville",
]

TENANT_COMPANIES = [
    ("Apex Distribution LLC", "Delaware", "Warehousing & Distribution"),
    ("GlobalTech Fulfillment Inc.", "California", "E-Commerce Fulfillment"),
    ("Midwest Logistics Partners", "Illinois", "Third-Party Logistics"),
    ("SunBelt Supply Chain Co.", "Texas", "Cold Chain Logistics"),
    ("Pacific Freight Solutions", "Nevada", "Freight Consolidation"),
    ("Eastern Seaboard Warehousing", "New Jersey", "Bulk Storage"),
    ("Heartland Manufacturing Corp.", "Ohio", "Light Manufacturing"),
    ("Southern Cross Logistics Ltd.", "Georgia", "Cross-Dock Operations"),
]

PROPERTY_ADDRESSES = [
    ("1500 Industrial Parkway", "Romeoville", "IL", "60446", "Will"),
    ("8200 Commerce Drive", "Irving", "TX", "75063", "Dallas"),
    ("3400 Logistics Boulevard", "McDonough", "GA", "30253", "Henry"),
    ("12000 Distribution Way", "Ontario", "CA", "91761", "San Bernardino"),
    ("500 Terminal Road", "Elizabeth", "NJ", "07201", "Union"),
    ("7800 Warehouse Lane", "Phoenix", "AZ", "85043", "Maricopa"),
    ("2200 Crossdock Circle", "Memphis", "TN", "38118", "Shelby"),
    ("9100 Fulfillment Drive", "Plainfield", "IN", "46168", "Hendricks"),
]

INSURANCE_CARRIERS = [
    "Hartford Financial Services", "Zurich Insurance Group",
    "Chubb Limited", "Liberty Mutual", "Travelers Companies",
]

GUARANTOR_NAMES = [
    "John R. Whitfield", "Margaret A. Chen", "Robert S. Patel",
    "Susan K. Yamamoto", "David L. Morrison", "Patricia M. O'Brien",
]

LAW_FIRMS = [
    "Baker McKenzie LLP", "Jones Day", "Kirkland & Ellis LLP",
    "DLA Piper LLP", "Latham & Watkins LLP",
]


def generate_lease_id():
    return f"CX-{random.randint(2024, 2026)}-{random.randint(10000, 99999)}"


def rand_dollar(low, high):
    return round(random.uniform(low, high), 2)


def rand_pct(low, high):
    return round(random.uniform(low, high), 2)


def rand_date_future(min_days=30, max_days=180):
    return datetime.now() + timedelta(days=random.randint(min_days, max_days))


def get_styles():
    """Custom styles matching Phase 1 conventions."""
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name='LeaseTitle', parent=styles['Heading1'],
        fontSize=16, alignment=TA_CENTER, spaceAfter=20,
        fontName='Helvetica-Bold'
    ))
    styles.add(ParagraphStyle(
        name='SectionHeader', parent=styles['Heading2'],
        fontSize=12, spaceBefore=15, spaceAfter=8,
        fontName='Helvetica-Bold'
    ))
    styles.add(ParagraphStyle(
        name='SubSection', parent=styles['Heading3'],
        fontSize=11, spaceBefore=10, spaceAfter=6,
        fontName='Helvetica-Bold'
    ))
    styles.add(ParagraphStyle(
        name='LeaseBody', parent=styles['Normal'],
        fontSize=10, alignment=TA_JUSTIFY, spaceAfter=8, leading=14
    ))
    styles.add(ParagraphStyle(
        name='LeaseClause', parent=styles['Normal'],
        fontSize=10, leftIndent=20, spaceAfter=6, leading=13
    ))
    return styles


# ---------------------------------------------------------------------------
# Data generators for each section
# ---------------------------------------------------------------------------

def gen_core_terms():
    """Section 1: Core Terms (~30 fields)."""
    tenant, tenant_state, tenant_business = random.choice(TENANT_COMPANIES)
    address, city, state, zipcode, county = random.choice(PROPERTY_ADDRESSES)
    market = random.choice(MARKETS)

    start = rand_date_future(30, 120)
    term_months = random.choice([60, 84, 120, 156, 180])
    end = start + relativedelta(months=term_months)
    execution_date = start - timedelta(days=random.randint(14, 60))
    rent_commence = start + relativedelta(months=random.randint(0, 3))

    sqft = random.randint(80000, 500000)
    land_acres = round(sqft / random.uniform(15000, 22000), 2)
    office_sqft = random.randint(2000, int(sqft * 0.05))
    warehouse_sqft = sqft - office_sqft
    clear_height = random.choice([32, 36, 40])
    dock_doors = random.randint(20, 80)
    drive_in_doors = random.randint(2, 6)
    trailer_parking = random.randint(40, 150)
    auto_parking = random.randint(50, 200)
    building_year = random.randint(2005, 2024)
    tax_parcel = f"{random.randint(10,99)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

    return {
        'lease_id': generate_lease_id(),
        'execution_date': execution_date.strftime('%B %d, %Y'),
        'landlord_name': 'Acme Industrial Real Estate, LLC',
        'landlord_state': 'Delaware',
        'landlord_entity_type': 'limited liability company',
        'landlord_address': '100 Corporate Center Drive, Suite 400, Wilmington, DE 19801',
        'tenant_name': tenant,
        'tenant_state': tenant_state,
        'tenant_entity_type': 'corporation' if 'Inc' in tenant or 'Corp' in tenant else 'limited liability company',
        'tenant_business_type': tenant_business,
        'tenant_address': f'{random.randint(100,9999)} {random.choice(["Commerce","Enterprise","Business"])} {random.choice(["Blvd","Ave","Pkwy"])}, {city}, {state} {zipcode}',
        'property_address': f'{address}, {city}, {state} {zipcode}',
        'property_city': city,
        'property_state': state,
        'property_zip': zipcode,
        'property_county': county,
        'market': market,
        'tax_parcel_id': tax_parcel,
        'total_rentable_sqft': sqft,
        'office_sqft': office_sqft,
        'warehouse_sqft': warehouse_sqft,
        'land_acres': land_acres,
        'clear_height_ft': clear_height,
        'dock_doors': dock_doors,
        'drive_in_doors': drive_in_doors,
        'trailer_parking_spaces': trailer_parking,
        'auto_parking_spaces': auto_parking,
        'building_year_built': building_year,
        'lease_start_date': start.strftime('%B %d, %Y'),
        'lease_end_date': end.strftime('%B %d, %Y'),
        'lease_term_months': term_months,
        'rent_commencement_date': rent_commence.strftime('%B %d, %Y'),
        'lease_execution_city': 'Wilmington, DE',
        '_start': start,
        '_end': end,
        '_sqft': sqft,
    }


def gen_financial(core):
    """Section 2: Financial (~50 fields)."""
    sqft = core['_sqft']
    base_rent_psf = rand_dollar(4.50, 9.00)
    annual_rent = round(sqft * base_rent_psf, 2)
    monthly_rent = round(annual_rent / 12, 2)
    escalation_pct = rand_pct(2.0, 4.0)
    escalation_type = random.choice(['Annual Fixed Percentage', 'CPI-Based', 'Stepped'])
    free_rent_months = random.randint(0, 6)
    security_deposit_months = random.randint(1, 3)
    security_deposit = round(monthly_rent * security_deposit_months, 2)
    lc_amount = round(monthly_rent * random.randint(3, 6), 2)
    lc_expiry_months = random.randint(12, 36)
    lc_burndown = random.choice(['Yes', 'No'])

    ti_psf = rand_dollar(15.00, 65.00)
    ti_total = round(sqft * ti_psf, 2)
    ti_deadline_months = random.randint(12, 24)
    ti_unused = random.choice(['Rent credit', 'Forfeited', 'Cash payment'])

    cam_psf = rand_dollar(1.20, 3.50)
    tax_psf = rand_dollar(0.80, 2.50)
    insurance_psf = rand_dollar(0.25, 0.75)
    mgmt_fee_pct = rand_pct(2.0, 5.0)

    yr2_rent = round(annual_rent * (1 + escalation_pct / 100), 2)
    yr3_rent = round(yr2_rent * (1 + escalation_pct / 100), 2)
    yr4_rent = round(yr3_rent * (1 + escalation_pct / 100), 2)
    yr5_rent = round(yr4_rent * (1 + escalation_pct / 100), 2)

    return {
        'base_rent_psf': base_rent_psf,
        'annual_base_rent': annual_rent,
        'monthly_base_rent': monthly_rent,
        'rent_escalation_pct': escalation_pct,
        'rent_escalation_type': escalation_type,
        'yr1_annual_rent': annual_rent,
        'yr2_annual_rent': yr2_rent,
        'yr3_annual_rent': yr3_rent,
        'yr4_annual_rent': yr4_rent,
        'yr5_annual_rent': yr5_rent,
        'rent_payment_day': random.choice([1, 5, 10, 15]),
        'rent_payment_method': random.choice(['Wire Transfer', 'ACH', 'Check']),
        'free_rent_months': free_rent_months,
        'free_rent_conditions': 'Applies to base rent only; NNN charges still payable' if free_rent_months > 0 else 'None',
        'security_deposit_amount': security_deposit,
        'security_deposit_months': security_deposit_months,
        'security_deposit_form': random.choice(['Cash', 'Letter of Credit', 'Cash or Letter of Credit']),
        'security_deposit_return_days': random.choice([30, 45, 60]),
        'security_deposit_interest': random.choice(['Yes, at money market rate', 'No']),
        'letter_of_credit_amount': lc_amount,
        'letter_of_credit_issuer_rating': 'Minimum A-rated financial institution',
        'letter_of_credit_expiry_months': lc_expiry_months,
        'letter_of_credit_burndown': lc_burndown,
        'letter_of_credit_draw_conditions': 'Monetary default uncured beyond notice period',
        'ti_allowance_psf': ti_psf,
        'ti_allowance_total': ti_total,
        'ti_deadline_months': ti_deadline_months,
        'ti_unused_treatment': ti_unused,
        'ti_approval_required': random.choice(['Yes, Landlord approval for plans', 'No, Tenant discretion']),
        'ti_general_contractor': random.choice(['Tenant selection, Landlord approval', 'Landlord-designated']),
        'ti_change_order_cap_pct': rand_pct(5.0, 15.0),
        'lease_type': random.choice(['Triple Net (NNN)', 'Modified Gross', 'Absolute Net']),
        'cam_psf_estimate': cam_psf,
        'cam_annual_estimate': round(sqft * cam_psf, 2),
        'tax_psf_estimate': tax_psf,
        'tax_annual_estimate': round(sqft * tax_psf, 2),
        'insurance_psf_estimate': insurance_psf,
        'insurance_annual_estimate': round(sqft * insurance_psf, 2),
        'mgmt_fee_pct': mgmt_fee_pct,
        'base_year': random.choice([2024, 2025, 2026]),
        'proration_method': random.choice(['Rentable square footage', 'Usable square footage']),
        'late_payment_fee_pct': rand_pct(3.0, 5.0),
        'late_payment_grace_days': random.choice([5, 7, 10]),
        'interest_on_late_payment_pct': rand_pct(8.0, 18.0),
        'holdover_rent_multiplier': random.choice([1.5, 2.0]),
        'holdover_rent_type': 'Percentage of then-current base rent',
    }


def gen_operating_expenses(core):
    """Section 3: Operating Expenses (~40 fields)."""
    sqft = core['_sqft']
    return {
        'opex_cap_pct': rand_pct(3.0, 7.0),
        'opex_cap_type': random.choice(['Cumulative compounding', 'Non-cumulative annual', 'None']),
        'opex_base_year_amount': round(sqft * rand_dollar(2.50, 4.50), 2),
        'opex_reconciliation_deadline_months': random.choice([3, 4, 6]),
        'opex_reconciliation_method': random.choice(['Actual costs', 'Estimated with true-up']),
        'opex_audit_right': 'Yes',
        'opex_audit_frequency': random.choice(['Once per year', 'Once per reconciliation period']),
        'opex_audit_notice_days': random.choice([30, 60, 90]),
        'opex_audit_period_years': random.choice([2, 3]),
        'opex_audit_cost_responsibility': random.choice([
            'Tenant bears cost unless overcharge exceeds 5%',
            'Landlord bears cost if overcharge exceeds 3%'
        ]),
        'opex_dispute_resolution': random.choice(['Binding arbitration', 'Mediation then litigation']),
        'opex_gross_up_provision': random.choice(['Yes, to 95% occupancy', 'Yes, to 100% occupancy', 'No']),
        'opex_gross_up_method': 'Variable expenses adjusted as if building were 95% occupied',
        'tax_protest_right': random.choice(['Tenant may protest, Landlord approval', 'Landlord protests only']),
        'tax_protest_cost_sharing': random.choice(['50/50', 'Pro rata', 'Tenant bears all costs']),
        'tax_abatement_sharing': random.choice(['Pro rata benefit to Tenant', 'Landlord retains']),
        'controllable_expense_cap_pct': rand_pct(3.0, 5.0),
        'uncontrollable_expenses_list': 'Real estate taxes, insurance premiums, utilities, snow removal',
        'utility_responsibility': random.choice(['Tenant direct-metered', 'Landlord submetered', 'Included in CAM']),
        'utility_types_covered': 'Electric, gas, water, sewer, telecom',
        'hvac_maintenance_responsibility': random.choice(['Tenant', 'Landlord', 'Shared']),
        'hvac_contract_requirement': random.choice(['Required, vendor approved by Landlord', 'Recommended']),
        'snow_removal_responsibility': random.choice(['Landlord via CAM', 'Tenant direct']),
        'landscaping_responsibility': random.choice(['Landlord via CAM', 'Tenant direct']),
        'janitorial_responsibility': 'Tenant',
        'pest_control_responsibility': random.choice(['Tenant', 'Landlord via CAM']),
        'trash_removal_responsibility': random.choice(['Tenant direct', 'Landlord via CAM']),
        'recycling_requirements': random.choice(['Mandatory per local ordinance', 'Voluntary']),
        'parking_lot_maintenance': random.choice(['Landlord via CAM', 'Tenant direct']),
        'roof_maintenance_responsibility': random.choice(['Landlord', 'Tenant for penetrations only']),
        'capital_expenditure_treatment': random.choice([
            'Amortized over useful life, pro rata to Tenant',
            'Excluded from operating expenses',
        ]),
        'capital_expenditure_threshold': f'${random.choice([5000, 10000, 25000]):,}',
        'reserve_fund_contribution_psf': rand_dollar(0.10, 0.50),
        'insurance_requirements_for_cam': 'Landlord maintains property policy; cost passed through',
        'property_management_company': random.choice([
            'Acme Property Management LLC',
            'Industrial Realty Management Corp.',
        ]),
        'property_management_fee_pct': rand_pct(2.0, 5.0),
        'administrative_overhead_pct': rand_pct(3.0, 10.0),
        'expense_exclusions': 'Leasing commissions, capital improvements not benefiting Tenant, Landlord legal fees',
        'tenant_proportionate_share_pct': round(random.uniform(30.0, 100.0), 2),
        'common_area_definition': 'All areas outside the Premises used in common by tenants and their invitees',
    }


def gen_options(core):
    """Section 4: Options (~45 fields)."""
    term_months = core['lease_term_months']
    return {
        'renewal_option_count': random.choice([1, 2, 3]),
        'renewal_option_term_months': random.choice([36, 60, 84]),
        'renewal_notice_months': random.choice([6, 9, 12]),
        'renewal_rent_method': random.choice([
            'Fair Market Value', '95% of Fair Market Value',
            'Fixed escalation from last year rent', 'CPI adjustment'
        ]),
        'renewal_fmv_determination': random.choice([
            'Mutual agreement, then broker determination',
            'Three-broker average', 'Baseball arbitration'
        ]),
        'renewal_fmv_dispute_resolution': 'Each party selects broker; brokers select third; middle value prevails',
        'renewal_ti_allowance': random.choice(['None', '$5.00 PSF', '$10.00 PSF']),
        'renewal_conditions': 'No uncured default at time of exercise',
        'expansion_option': random.choice(['Yes', 'No']),
        'expansion_space_sqft': random.randint(20000, 100000),
        'expansion_notice_months': random.choice([6, 9, 12]),
        'expansion_rent_rate': random.choice(['Same terms as Premises', 'Fair Market Value']),
        'expansion_deadline_month': random.choice([24, 36, 48]),
        'expansion_ti_allowance': random.choice(['Pro rata of original TI', 'None', '$10.00 PSF']),
        'rofo_right': random.choice(['Yes', 'No']),
        'rofo_space_description': 'Any contiguous space in the building or adjacent buildings',
        'rofo_notice_days': random.choice([10, 15, 30]),
        'rofo_response_days': random.choice([10, 15, 20]),
        'rofo_matching_terms': random.choice(['Match third-party terms', '95% of third-party terms']),
        'rofr_right': random.choice(['Yes', 'No']),
        'rofr_space_description': 'Building and adjacent parcels',
        'rofr_notice_days': random.choice([10, 15, 30]),
        'purchase_option': random.choice(['Yes', 'No']),
        'purchase_option_price_method': random.choice([
            'Fair Market Value at time of exercise',
            'Fixed price of $X per square foot',
            'Formula-based: NOI / cap rate'
        ]),
        'purchase_option_exercise_window': f'Month {random.choice([36, 48, 60])} through Month {random.choice([72, 84, 96])}',
        'purchase_option_due_diligence_days': random.choice([30, 45, 60]),
        'purchase_option_closing_days': random.choice([60, 90, 120]),
        'termination_option': random.choice(['Yes', 'No']),
        'termination_option_effective_month': random.choice([36, 48, 60]),
        'termination_notice_months': random.choice([6, 9, 12]),
        'termination_fee_months_rent': random.choice([3, 6, 9, 12]),
        'termination_fee_includes_unamortized_ti': random.choice(['Yes', 'No']),
        'termination_fee_includes_commission': random.choice(['Yes', 'No']),
        'contraction_option': random.choice(['Yes', 'No']),
        'contraction_min_sqft_retained': random.randint(30000, 80000),
        'contraction_notice_months': random.choice([9, 12]),
        'contraction_fee_type': random.choice(['Unamortized TI + commissions', 'Fixed fee per sqft released']),
        'relocation_right_landlord': random.choice(['Yes, with 90 days notice', 'No']),
        'relocation_comparable_space': 'Comparable size, location, and condition within Landlord portfolio',
        'relocation_cost_responsibility': 'Landlord bears all relocation costs',
        'must_take_space': random.choice(['Yes', 'No']),
        'must_take_space_sqft': random.randint(10000, 50000),
        'must_take_deadline_month': random.choice([12, 24, 36]),
        'must_take_rent_rate': 'Same terms as initial Premises',
        'sublease_consent_required': random.choice(['Yes, not unreasonably withheld', 'No, freely assignable']),
        'sublease_profit_sharing_pct': rand_pct(0.0, 50.0),
    }


def gen_insurance_liability():
    """Section 5: Insurance & Liability (~40 fields)."""
    carrier = random.choice(INSURANCE_CARRIERS)
    return {
        'gl_coverage_per_occurrence': f'${random.choice([1000000, 2000000, 5000000]):,}',
        'gl_coverage_aggregate': f'${random.choice([2000000, 5000000, 10000000]):,}',
        'gl_deductible_max': f'${random.choice([5000, 10000, 25000]):,}',
        'property_insurance_coverage': random.choice(['Replacement cost', 'Actual cash value']),
        'property_insurance_includes_ti': 'Yes',
        'property_insurance_business_personal_property': 'Full replacement cost',
        'umbrella_excess_liability': f'${random.choice([5000000, 10000000, 25000000]):,}',
        'workers_comp_coverage': 'Statutory limits',
        'auto_liability_coverage': f'${random.choice([1000000, 2000000]):,}',
        'business_interruption_coverage_months': random.choice([6, 12, 18]),
        'professional_liability_required': random.choice(['Yes', 'No']),
        'environmental_liability_coverage': f'${random.choice([1000000, 5000000]):,}',
        'tenant_insurance_carrier_rating': 'A.M. Best A- VII or better',
        'landlord_additional_insured': 'Yes',
        'landlord_lender_additional_insured': 'Yes, as required by Landlord',
        'insurance_certificate_delivery_days': random.choice([10, 15, 30]),
        'insurance_renewal_notice_days': random.choice([15, 30]),
        'waiver_of_subrogation': 'Mutual',
        'waiver_of_subrogation_scope': 'All property and casualty claims',
        'indemnification_by_tenant': 'Tenant indemnifies Landlord for claims arising from Tenant use or occupancy',
        'indemnification_by_landlord': 'Landlord indemnifies Tenant for claims arising from Landlord negligence',
        'indemnification_survival_months': random.choice([12, 24, 36]),
        'indemnification_cap': random.choice(['None', 'Limited to insurance proceeds', '$5,000,000']),
        'mutual_waiver_of_consequential_damages': random.choice(['Yes', 'No']),
        'landlord_liability_cap': random.choice(['Landlord equity in property', 'Insurance proceeds only', 'None']),
        'tenant_liability_cap': random.choice(['None', 'Two years base rent']),
        'hold_harmless_scope': 'Each party for its own acts, omissions, and negligence',
        'insurance_increase_due_to_tenant_use': 'Tenant reimburses premium increase',
        'landlord_property_insurance_type': 'All-risk, replacement cost, including terrorism',
        'landlord_property_insurance_deductible': f'${random.choice([25000, 50000, 100000]):,}',
        'earthquake_insurance': random.choice(['Included', 'Excluded', 'Optional at Tenant cost']),
        'flood_insurance': random.choice(['Included', 'Excluded — not in flood zone', 'Required per FEMA']),
        'terrorism_insurance': 'Included per TRIA',
        'cyber_liability_required': random.choice(['Yes, $1,000,000 minimum', 'No']),
        'pollution_legal_liability': random.choice(['Required', 'Not required']),
        'builders_risk_during_ti': random.choice(['Tenant provides', 'Landlord provides']),
        'blanket_policy_acceptable': 'Yes, with dedicated limits per location',
        'self_insurance_permitted': random.choice([
            'Yes, if net worth exceeds $500M',
            'No, third-party coverage required'
        ]),
        'insurance_review_frequency': random.choice(['Annually', 'Every 3 years']),
        'insurance_adjustment_for_inflation': random.choice(['Yes, CPI-based', 'At Landlord discretion']),
    }


def gen_construction_alterations(core):
    """Section 6: Construction & Alterations (~35 fields)."""
    sqft = core['_sqft']
    return {
        'initial_buildout_responsibility': random.choice(['Tenant', 'Landlord per approved plans']),
        'initial_buildout_deadline_days': random.choice([180, 240, 365]),
        'buildout_plan_approval_days': random.choice([15, 20, 30]),
        'buildout_plan_resubmission_days': random.choice([10, 15]),
        'construction_manager': random.choice(['Tenant-selected, Landlord-approved', 'Landlord-designated']),
        'construction_oversight_fee_pct': rand_pct(2.0, 5.0),
        'prevailing_wage_required': random.choice(['Yes', 'No', 'Per local ordinance']),
        'construction_insurance_requirements': 'Builders risk + contractor GL per lease requirements',
        'construction_lien_waiver_required': 'Yes, conditional and unconditional',
        'construction_completion_guarantee': random.choice(['Performance bond', 'Letter of credit', 'None']),
        'alterations_threshold_no_approval': f'${random.choice([10000, 25000, 50000]):,}',
        'alterations_structural_consent': 'Required for any structural modifications',
        'alterations_cosmetic_consent': random.choice(['Not required', 'Notice to Landlord only']),
        'alterations_removal_at_expiration': random.choice([
            'Landlord may require removal',
            'All alterations become Landlord property',
        ]),
        'alterations_restoration_obligation': random.choice(['Yes, to shell condition', 'Negotiated at lease end']),
        'restoration_deposit_required': random.choice(['Yes', 'No']),
        'restoration_cost_estimate': f'${round(sqft * rand_dollar(1.00, 3.00), 2):,.2f}',
        'signage_right': random.choice(['Building-mounted and monument', 'Monument only', 'Suite entry only']),
        'signage_size_max_sqft': random.choice([50, 100, 200]),
        'signage_approval_required': 'Yes, design and placement subject to Landlord approval',
        'signage_cost_responsibility': 'Tenant',
        'signage_removal_at_expiration': 'Tenant removes and restores',
        'telecom_riser_access': random.choice(['Dedicated riser', 'Shared riser']),
        'telecom_provider_choice': 'Tenant selects, Landlord approval not unreasonably withheld',
        'telecom_equipment_rooftop': random.choice(['Permitted with license', 'Not permitted']),
        'rooftop_license_fee_monthly': f'${random.choice([500, 1000, 1500]):,}',
        'generator_permitted': random.choice(['Yes, pad-mounted', 'No']),
        'generator_fuel_type': random.choice(['Diesel', 'Natural gas']),
        'generator_noise_restrictions': 'Must comply with local noise ordinance',
        'solar_panel_permitted': random.choice(['Yes, with Landlord approval', 'No']),
        'ev_charging_stations_permitted': random.choice(['Yes', 'No']),
        'racking_system_approval': random.choice(['Required for systems over 20 feet', 'Not required']),
        'floor_load_capacity_psf': random.choice([250, 300, 500]),
        'mezzanine_permitted': random.choice(['Yes, with structural review', 'No']),
        'hazmat_storage_modifications': 'Subject to Environmental section requirements',
    }


def gen_default_remedies():
    """Section 7: Default & Remedies (~40 fields)."""
    return {
        'monetary_default_cure_days': random.choice([5, 7, 10]),
        'non_monetary_default_cure_days': random.choice([30, 45, 60]),
        'non_monetary_extended_cure': random.choice([
            'Additional 30 days if diligently pursuing cure',
            'Up to 90 days total with Landlord approval'
        ]),
        'notice_of_default_method': 'Certified mail, return receipt requested, and email',
        'notice_of_default_address_tenant': 'Premises address and corporate headquarters',
        'notice_of_default_address_landlord': 'Acme Industrial Real Estate, LLC, 100 Corporate Center Dr, Suite 400, Wilmington, DE 19801',
        'late_fee_percentage': rand_pct(3.0, 5.0),
        'late_fee_grace_period_days': random.choice([5, 7, 10]),
        'interest_on_past_due_rate': rand_pct(8.0, 18.0),
        'interest_calculation_method': random.choice(['Simple interest', 'Compounding monthly']),
        'landlord_lien_on_property': random.choice(['Yes, contractual lien', 'Waived']),
        'landlord_lockout_right': random.choice(['Yes, after default and cure period', 'No']),
        'landlord_self_help_right': 'Yes, for emergency repairs with reasonable notice',
        'cross_default_provision': random.choice(['Yes, with all Landlord affiliates', 'No']),
        'cross_default_cure_period_days': random.choice([10, 15, 30]),
        'acceleration_of_rent': random.choice([
            'Landlord may accelerate all remaining rent upon uncured default',
            'Limited to 12 months rent',
        ]),
        'mitigation_of_damages': 'Landlord shall use commercially reasonable efforts to mitigate',
        'consequential_damages_waiver': random.choice(['Mutual waiver', 'No waiver']),
        'attorneys_fees_prevailing_party': 'Yes',
        'attorneys_fees_cap': random.choice(['None', '$50,000', '$100,000']),
        'guarantor_name': random.choice(GUARANTOR_NAMES),
        'guarantor_relationship': random.choice(['CEO', 'Principal Owner', 'President']),
        'guarantee_type': random.choice(['Full recourse', 'Limited - declining over time', 'Good-guy guarantee']),
        'guarantee_amount_cap': random.choice(['Unlimited', '24 months rent', '12 months rent + TI']),
        'guarantee_burndown_schedule': random.choice([
            'Reduces 25% per year', 'Reduces upon meeting revenue thresholds', 'No burndown'
        ]),
        'guarantee_financial_reporting': random.choice(['Annual personal financial statement', 'Quarterly']),
        'bankruptcy_provision': 'Default upon voluntary filing; involuntary filing cured within 60 days',
        'bankruptcy_adequate_assurance_days': random.choice([30, 60]),
        'right_to_cure_by_lender': random.choice(['Yes, additional 30 days', 'No']),
        'right_to_cure_by_guarantor': random.choice(['Yes, additional 15 days', 'No']),
        'surrender_condition': random.choice(['Broom clean, good condition', 'Shell condition']),
        'surrender_inspection_days_before': random.choice([30, 60, 90]),
        'holdover_provision': 'Month-to-month at holdover rent multiplier',
        'holdover_notice_to_vacate_days': random.choice([30, 60]),
        'landlord_default_notice_days': random.choice([30, 45, 60]),
        'landlord_default_cure_days': random.choice([30, 60, 90]),
        'rent_abatement_for_landlord_default': random.choice(['Yes, after notice and cure', 'No']),
        'tenant_offset_right': random.choice(['Yes, for uncured Landlord defaults', 'No']),
        'force_majeure_rent_abatement': random.choice(['Yes', 'No']),
        'dispute_resolution_method': random.choice(['Binding arbitration', 'Litigation', 'Mediation then arbitration']),
    }


def gen_environmental_compliance():
    """Section 8: Environmental & Compliance (~35 fields)."""
    return {
        'hazmat_permitted': random.choice(['Limited to ordinary business quantities', 'Prohibited', 'With Landlord approval']),
        'hazmat_types_permitted': 'Cleaning supplies, forklift fuel, office chemicals in de minimis quantities',
        'hazmat_storage_requirements': 'Secondary containment, MSDS on-site, compliant with RCRA',
        'hazmat_reporting_frequency': random.choice(['Quarterly', 'Annually', 'Upon change']),
        'hazmat_removal_at_expiration': 'Tenant shall remove all hazardous materials and certify removal',
        'phase_i_esa_baseline': random.choice(['Completed by Landlord, shared with Tenant', 'Tenant may conduct at own cost']),
        'phase_i_esa_date': rand_date_future(-365, -30).strftime('%B %d, %Y'),
        'phase_ii_esa_required': random.choice(['If Phase I recommends', 'Not required']),
        'environmental_indemnification_by_tenant': 'For contamination caused by Tenant during lease term',
        'environmental_indemnification_by_landlord': 'For pre-existing contamination',
        'environmental_indemnification_survival_years': random.choice([3, 5, 10]),
        'environmental_remediation_responsibility': random.choice([
            'Landlord for pre-existing, Tenant for Tenant-caused',
            'Tenant for all during lease term',
        ]),
        'environmental_remediation_standard': random.choice([
            'Industrial use standard', 'Residential use standard (higher)'
        ]),
        'environmental_insurance_required': random.choice(['Yes', 'No']),
        'asbestos_survey_completed': random.choice(['Yes, no ACM found', 'Yes, ACM managed in place', 'Not applicable']),
        'lead_paint_disclosure': random.choice(['Not applicable — built after 1978', 'Disclosure provided']),
        'mold_prevention_responsibility': 'Tenant shall maintain HVAC and report water intrusion within 24 hours',
        'indoor_air_quality_standards': random.choice(['ASHRAE 62.1 compliance', 'Not specified']),
        'stormwater_management_compliance': 'Tenant shall comply with NPDES permit requirements',
        'spcc_plan_required': random.choice(['Yes, if oil storage exceeds 1,320 gallons', 'Not applicable']),
        'ada_compliance_responsibility': random.choice([
            'Landlord for base building, Tenant for Premises',
            'Tenant for all within Premises',
        ]),
        'ada_compliance_cost_sharing': random.choice(['Tenant 100%', 'Shared pro rata']),
        'fire_code_compliance': 'Tenant shall maintain fire suppression and comply with NFPA standards',
        'fire_sprinkler_system': random.choice(['ESFR', 'In-rack required', 'Wet system adequate']),
        'fire_alarm_monitoring': 'Required, Tenant-contracted with approved monitoring company',
        'zoning_compliance_warranty_landlord': 'Landlord warrants current zoning permits industrial use',
        'zoning_current_classification': random.choice(['I-1 Light Industrial', 'I-2 Heavy Industrial', 'M-1 Manufacturing']),
        'zoning_special_use_permit': random.choice(['Not required', 'Required for cold storage', 'Required for hazmat']),
        'building_code_compliance': 'Tenant improvements shall comply with current building code',
        'energy_code_compliance': random.choice(['ASHRAE 90.1', 'Local energy code', 'LEED Silver equivalent']),
        'sustainability_requirements': random.choice([
            'LED lighting, recycling program required',
            'None specified',
            'LEED certification maintenance required',
        ]),
        'noise_restrictions': 'Comply with local noise ordinance; no outdoor operations 10PM-6AM',
        'operating_hours_restrictions': random.choice(['24/7 permitted', '6AM-10PM only', 'No restrictions']),
        'truck_traffic_restrictions': random.choice([
            'Designated routes only', 'No restrictions', 'Peak hours restricted 7-9AM, 4-6PM'
        ]),
        'odor_emission_restrictions': 'No odors detectable beyond property line',
    }


def gen_miscellaneous(core):
    """Section 9: Miscellaneous (~36 fields)."""
    law_firm = random.choice(LAW_FIRMS)
    return {
        'governing_law_state': core['property_state'],
        'jurisdiction_venue': f'{core["property_county"]} County, {core["property_state"]}',
        'force_majeure_definition': 'Acts of God, war, terrorism, pandemic, government orders, labor strikes, supply chain disruption',
        'force_majeure_max_days': random.choice([180, 270, 365]),
        'force_majeure_rent_obligation': random.choice(['Continues', 'Abated after 30 days']),
        'subordination_required': 'Yes, to existing and future mortgages',
        'subordination_non_disturbance': 'Landlord shall deliver SNDA from lender within 30 days of lease execution',
        'snda_form': random.choice(['Lender standard form', 'Negotiated form']),
        'attornment_obligation': 'Tenant shall attorn to any successor landlord',
        'estoppel_certificate_delivery_days': random.choice([10, 15, 20]),
        'estoppel_certificate_frequency': random.choice(['Upon request', 'Maximum twice annually']),
        'estoppel_certificate_content': 'Lease terms, rent, defaults, commencement date, expiration date',
        'recording_of_lease': random.choice(['Memorandum of lease to be recorded', 'Not recorded']),
        'recording_cost_responsibility': random.choice(['Tenant', 'Landlord', 'Shared equally']),
        'broker_landlord': random.choice(['CBRE', 'JLL', 'Cushman & Wakefield', 'Colliers', 'Newmark']),
        'broker_tenant': random.choice(['Prologis Advisory', 'Lee & Associates', 'NAI Global', 'Marcus & Millichap']),
        'broker_commission_responsibility': 'Landlord per separate agreement',
        'broker_commission_on_renewal': random.choice(['Yes, reduced rate', 'No']),
        'quiet_enjoyment_covenant': 'Landlord covenants Tenant shall peacefully hold and enjoy the Premises',
        'access_by_landlord': 'Reasonable prior notice (24 hours), except emergencies',
        'landlord_access_hours': random.choice(['Business hours only', 'Any reasonable time with notice']),
        'signage_on_building_directory': random.choice(['Included', 'Not applicable']),
        'parking_allocation': f'{core["auto_parking_spaces"]} auto spaces, {core["trailer_parking_spaces"]} trailer spaces',
        'confidentiality_of_lease_terms': random.choice(['Mutual confidentiality', 'Not confidential']),
        'entire_agreement_clause': 'This Lease constitutes the entire agreement; supersedes all prior negotiations',
        'amendment_requirements': 'Written instrument signed by both parties',
        'severability_clause': 'Standard — invalid provisions severed without affecting remainder',
        'waiver_of_jury_trial': random.choice(['Mutual waiver', 'No waiver']),
        'notices_delivery_method': 'Certified mail, nationally recognized overnight courier, and email',
        'notices_deemed_received': random.choice([
            '3 business days after mailing',
            'Next business day for overnight courier',
        ]),
        'assignment_consent_required': random.choice(['Yes, not unreasonably withheld', 'Freely assignable']),
        'assignment_release_of_assignor': random.choice(['Yes, upon Landlord approval', 'No, assignor remains liable']),
        'transfer_fee': random.choice(['None', '$5,000 administrative fee']),
        'tenant_financial_reporting': random.choice(['Annual audited statements', 'Quarterly unaudited']),
        'landlord_lender_name': random.choice(['JPMorgan Chase', 'Wells Fargo', 'Bank of America', 'Goldman Sachs']),
        'exhibit_list': 'A (Floor Plan), B (Work Letter), C (Rules & Regulations), D (SNDA Form), E (Guaranty)',
    }


# ---------------------------------------------------------------------------
# PDF builder
# ---------------------------------------------------------------------------

def _p(story, text, style):
    """Shorthand: append a paragraph to the story."""
    story.append(Paragraph(text, style))


def _sp(story, inches=0.1):
    """Shorthand: append a spacer."""
    story.append(Spacer(1, inches * inch))


def build_lease_pdf(data, filename):
    """Build a multi-page lease PDF with realistic legal prose.

    Values are embedded in narrative paragraphs rather than listed as
    label-value pairs, mimicking the structure of actual commercial
    industrial lease agreements.
    """
    filepath = os.path.join(OUTPUT_DIR, filename)
    styles = get_styles()
    d = data  # shorter alias

    doc = SimpleDocTemplate(
        filepath, pagesize=letter,
        leftMargin=0.75 * inch, rightMargin=0.75 * inch,
        topMargin=0.75 * inch, bottomMargin=0.75 * inch,
    )

    story = []

    # ── Title Page ──
    _p(story, "INDUSTRIAL LEASE AGREEMENT", styles['LeaseTitle'])
    _p(story,
       f"This Industrial Lease Agreement (this \"Lease\"), identified as Lease "
       f"No. <b>{d['lease_id']}</b>, is entered into as of <b>{d['execution_date']}</b> "
       f"in {d['lease_execution_city']}, by and between the parties identified below.",
       styles['LeaseBody'])
    _sp(story, 0.2)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 1: PARTIES
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 1: PARTIES", styles['SectionHeader'])

    _p(story,
       f"1.1 <b>Landlord.</b> {d['landlord_name']}, a {d['landlord_state']} "
       f"{d['landlord_entity_type']} (\"Landlord\"), with its principal office "
       f"located at {d['landlord_address']}.",
       styles['LeaseBody'])

    _p(story,
       f"1.2 <b>Tenant.</b> {d['tenant_name']}, a {d['tenant_state']} "
       f"{d['tenant_entity_type']} (\"Tenant\"), primarily engaged in the business "
       f"of {d['tenant_business_type']}, with its principal office located at "
       f"{d['tenant_address']}.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 2: PREMISES
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 2: PREMISES", styles['SectionHeader'])

    _p(story,
       f"2.1 <b>Demised Premises.</b> Landlord hereby leases to Tenant and Tenant "
       f"hereby leases from Landlord certain premises located at "
       f"{d['property_address']} (the \"Premises\"), "
       f"situated in the City of {d['property_city']}, "
       f"{d['property_county']} County, State of {d['property_state']}, "
       f"ZIP Code {d['property_zip']}, within the {d['market']} industrial market. "
       f"The property is identified by Tax Parcel ID {d['tax_parcel_id']} in the "
       f"records of {d['property_county']} County.",
       styles['LeaseBody'])

    _p(story,
       f"2.2 <b>Building Description.</b> The Premises consist of an industrial "
       f"building originally constructed in {d['building_year_built']}, containing "
       f"approximately {d['total_rentable_sqft']:,} rentable square feet, of which "
       f"approximately {d['warehouse_sqft']:,} square feet are designated for "
       f"warehouse and distribution use and approximately {d['office_sqft']:,} "
       f"square feet are designated for office use. The building is situated on "
       f"approximately {d['land_acres']} acres of land.",
       styles['LeaseBody'])

    _p(story,
       f"2.3 <b>Building Specifications.</b> The warehouse portion of the Premises "
       f"provides a clear height of {d['clear_height_ft']} feet, with "
       f"{d['dock_doors']} dock-high loading doors and {d['drive_in_doors']} "
       f"drive-in doors at grade level. The site includes {d['trailer_parking_spaces']} "
       f"trailer parking spaces and {d['auto_parking_spaces']} automobile parking spaces.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 3: TERM
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 3: LEASE TERM", styles['SectionHeader'])

    _p(story,
       f"3.1 <b>Term.</b> The term of this Lease (the \"Lease Term\") shall be for "
       f"a period of {d['lease_term_months']} months, commencing on "
       f"{d['lease_start_date']} (the \"Commencement Date\") and expiring on "
       f"{d['lease_end_date']} (the \"Expiration Date\"), unless sooner terminated "
       f"or extended in accordance with the provisions of this Lease.",
       styles['LeaseBody'])

    _p(story,
       f"3.2 <b>Rent Commencement.</b> Tenant's obligation to pay Base Rent shall "
       f"commence on {d['rent_commencement_date']} (the \"Rent Commencement Date\"). "
       f"All other obligations of Tenant under this Lease, including the obligation "
       f"to pay Additional Rent, shall commence on the Commencement Date.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 4: FINANCIAL TERMS
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 4: RENT AND FINANCIAL TERMS", styles['SectionHeader'])

    _p(story,
       f"4.1 <b>Base Rent.</b> Commencing on the Rent Commencement Date, Tenant "
       f"shall pay to Landlord annual Base Rent in the amount of "
       f"${d['annual_base_rent']:,.2f}, payable in equal monthly installments of "
       f"${d['monthly_base_rent']:,.2f}, calculated at the rate of "
       f"${d['base_rent_psf']:.2f} per rentable square foot per annum. Base Rent "
       f"shall be due and payable on the {d['rent_payment_day']}th day of each "
       f"calendar month, in advance, via {d['rent_payment_method']}.",
       styles['LeaseBody'])

    _p(story,
       f"4.2 <b>Rent Escalation.</b> On each anniversary of the Rent Commencement "
       f"Date, the annual Base Rent shall increase by {d['rent_escalation_pct']}% "
       f"pursuant to the {d['rent_escalation_type']} method. The projected rent "
       f"schedule for the first five (5) Lease Years is as follows: Year 1: "
       f"${d['yr1_annual_rent']:,.2f}; Year 2: ${d['yr2_annual_rent']:,.2f}; "
       f"Year 3: ${d['yr3_annual_rent']:,.2f}; Year 4: "
       f"${d['yr4_annual_rent']:,.2f}; Year 5: ${d['yr5_annual_rent']:,.2f}.",
       styles['LeaseBody'])

    fr_text = (
        f"4.3 <b>Rent Abatement.</b> Notwithstanding anything to the contrary "
        f"herein, Landlord shall grant Tenant an abatement of Base Rent for a "
        f"period of {d['free_rent_months']} month(s) following the Rent "
        f"Commencement Date. {d['free_rent_conditions']}."
    ) if d['free_rent_months'] > 0 else (
        f"4.3 <b>Rent Abatement.</b> There shall be no abatement of Base Rent "
        f"under this Lease."
    )
    _p(story, fr_text, styles['LeaseBody'])

    _p(story,
       f"4.4 <b>Security Deposit.</b> Upon execution of this Lease, Tenant shall "
       f"deliver to Landlord a security deposit in the amount of "
       f"${d['security_deposit_amount']:,.2f}, representing approximately "
       f"{d['security_deposit_months']} month(s) of Base Rent. The security "
       f"deposit shall be in the form of {d['security_deposit_form']}. "
       f"Interest on the deposit: {d['security_deposit_interest']}. "
       f"Provided Tenant is not in default, Landlord shall return the security "
       f"deposit within {d['security_deposit_return_days']} days following the "
       f"Expiration Date, less any amounts applied in accordance with this Lease.",
       styles['LeaseBody'])

    _p(story,
       f"4.5 <b>Letter of Credit.</b> In addition to the security deposit, Tenant "
       f"shall deliver to Landlord an irrevocable standby letter of credit in the "
       f"amount of ${d['letter_of_credit_amount']:,.2f}, issued by a financial "
       f"institution with a minimum rating of {d['letter_of_credit_issuer_rating']}. "
       f"The letter of credit shall have an initial term of "
       f"{d['letter_of_credit_expiry_months']} months and shall be subject to "
       f"burndown: {d['letter_of_credit_burndown']}. Landlord may draw upon the "
       f"letter of credit upon the occurrence of: {d['letter_of_credit_draw_conditions']}.",
       styles['LeaseBody'])

    _p(story,
       f"4.6 <b>Tenant Improvement Allowance.</b> Landlord shall provide Tenant "
       f"with a Tenant Improvement Allowance (\"TI Allowance\") in the amount of "
       f"${d['ti_allowance_psf']:.2f} per rentable square foot, for a total "
       f"allowance of ${d['ti_allowance_total']:,.2f}. Tenant must submit "
       f"improvement plans for completion within {d['ti_deadline_months']} months "
       f"of the Commencement Date. Plan approval: {d['ti_approval_required']}. "
       f"General contractor selection: {d['ti_general_contractor']}. "
       f"Change orders shall not exceed {d['ti_change_order_cap_pct']}% of the "
       f"original construction contract amount without Landlord's prior written "
       f"consent. Any unused portion of the TI Allowance shall be treated as "
       f"follows: {d['ti_unused_treatment']}.",
       styles['LeaseBody'])

    _p(story,
       f"4.7 <b>Lease Type and Additional Charges.</b> This Lease is a "
       f"{d['lease_type']} lease. In addition to Base Rent, Tenant shall pay its "
       f"proportionate share of operating expenses, real estate taxes, and "
       f"insurance premiums as Additional Rent. Estimated charges for the initial "
       f"Lease Year are as follows: common area maintenance at ${d['cam_psf_estimate']:.2f} "
       f"per RSF (${d['cam_annual_estimate']:,.2f} annually); real estate taxes at "
       f"${d['tax_psf_estimate']:.2f} per RSF (${d['tax_annual_estimate']:,.2f} "
       f"annually); and insurance at ${d['insurance_psf_estimate']:.2f} per RSF "
       f"(${d['insurance_annual_estimate']:,.2f} annually). A management fee of "
       f"{d['mgmt_fee_pct']}% of collected rents shall be included in operating "
       f"expenses. The base year for expense stop calculations is calendar year "
       f"{d['base_year']}. Tenant's proportionate share shall be calculated using "
       f"the {d['proration_method']} method.",
       styles['LeaseBody'])

    _p(story,
       f"4.8 <b>Late Charges.</b> Any installment of Rent not received by "
       f"Landlord within {d['late_payment_grace_days']} days of the date due shall "
       f"bear a late charge of {d['late_payment_fee_pct']}% of the overdue amount "
       f"and shall accrue interest at the rate of {d['interest_on_late_payment_pct']}% "
       f"per annum from the date due until paid. In the event Tenant holds over "
       f"after the Expiration Date, holdover rent shall be {d['holdover_rent_multiplier']}x "
       f"the then-current Base Rent, calculated as {d['holdover_rent_type']}.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 5: OPERATING EXPENSES
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 5: OPERATING EXPENSES", styles['SectionHeader'])

    _p(story,
       f"5.1 <b>Expense Cap.</b> Annual increases in Tenant's share of operating "
       f"expenses shall not exceed {d['opex_cap_pct']}% per annum, calculated on "
       f"a {d['opex_cap_type']} basis. The base year operating expense amount is "
       f"${d['opex_base_year_amount']:,.2f}. Controllable expenses shall be "
       f"separately capped at {d['controllable_expense_cap_pct']}% annual "
       f"increases. Uncontrollable expenses include: {d['uncontrollable_expenses_list']}.",
       styles['LeaseBody'])

    _p(story,
       f"5.2 <b>Reconciliation.</b> Within {d['opex_reconciliation_deadline_months']} "
       f"months following the end of each calendar year, Landlord shall furnish "
       f"Tenant with a statement of actual operating expenses using the "
       f"{d['opex_reconciliation_method']} method. Tenant's proportionate share "
       f"of operating expenses is {d['tenant_proportionate_share_pct']}%. For "
       f"purposes of this Lease, \"Common Areas\" means {d['common_area_definition']}.",
       styles['LeaseBody'])

    _p(story,
       f"5.3 <b>Gross-Up.</b> {d['opex_gross_up_provision']}. Where applicable, "
       f"the gross-up methodology shall be as follows: {d['opex_gross_up_method']}.",
       styles['LeaseBody'])

    _p(story,
       f"5.4 <b>Audit Rights.</b> Tenant shall have the right to audit Landlord's "
       f"operating expense records ({d['opex_audit_right']}). Such audit may be "
       f"conducted {d['opex_audit_frequency']}, upon {d['opex_audit_notice_days']} "
       f"days' prior written notice, and may cover the preceding "
       f"{d['opex_audit_period_years']} year(s). {d['opex_audit_cost_responsibility']}. "
       f"Any disputes arising from such audit shall be resolved by "
       f"{d['opex_dispute_resolution']}.",
       styles['LeaseBody'])

    _p(story,
       f"5.5 <b>Tax Protest.</b> {d['tax_protest_right']}. Costs of any tax "
       f"protest shall be shared as follows: {d['tax_protest_cost_sharing']}. "
       f"Any resulting tax abatement shall be shared: {d['tax_abatement_sharing']}.",
       styles['LeaseBody'])

    _p(story,
       f"5.6 <b>Utilities and Services.</b> Utility responsibility: "
       f"{d['utility_responsibility']}. Utility types covered include "
       f"{d['utility_types_covered']}. HVAC maintenance shall be the "
       f"responsibility of {d['hvac_maintenance_responsibility']}; HVAC service "
       f"contract: {d['hvac_contract_requirement']}. The following services shall "
       f"be allocated as indicated: snow removal ({d['snow_removal_responsibility']}), "
       f"landscaping ({d['landscaping_responsibility']}), janitorial "
       f"({d['janitorial_responsibility']}), pest control "
       f"({d['pest_control_responsibility']}), trash removal "
       f"({d['trash_removal_responsibility']}), and recycling "
       f"({d['recycling_requirements']}). Parking lot maintenance shall be "
       f"{d['parking_lot_maintenance']}. Roof maintenance responsibility: "
       f"{d['roof_maintenance_responsibility']}.",
       styles['LeaseBody'])

    _p(story,
       f"5.7 <b>Capital Expenditures and Management.</b> Capital expenditures "
       f"shall be treated as follows: {d['capital_expenditure_treatment']}. "
       f"Capital expenditure threshold: {d['capital_expenditure_threshold']}. "
       f"A reserve fund contribution of ${d['reserve_fund_contribution_psf']:.2f} "
       f"per RSF shall be collected annually. {d['insurance_requirements_for_cam']}. "
       f"The Premises shall be managed by {d['property_management_company']}, "
       f"with a management fee of {d['property_management_fee_pct']}% and an "
       f"administrative overhead charge of {d['administrative_overhead_pct']}%. "
       f"The following items are expressly excluded from operating expenses: "
       f"{d['expense_exclusions']}.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 6: OPTIONS
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 6: OPTIONS", styles['SectionHeader'])

    _p(story,
       f"6.1 <b>Renewal Options.</b> Provided Tenant is not in default and "
       f"subject to the condition that {d['renewal_conditions']}, Tenant shall "
       f"have {d['renewal_option_count']} option(s) to extend the Lease Term for "
       f"an additional period of {d['renewal_option_term_months']} months each, "
       f"exercisable by delivering written notice to Landlord not less than "
       f"{d['renewal_notice_months']} months prior to the then-current Expiration "
       f"Date. Base Rent during each renewal period shall be determined by "
       f"{d['renewal_rent_method']}. If the parties are unable to agree on Fair "
       f"Market Value, it shall be determined by the following process: "
       f"{d['renewal_fmv_determination']}. In the event of a dispute, "
       f"{d['renewal_fmv_dispute_resolution']}. Tenant Improvement Allowance "
       f"during the renewal period: {d['renewal_ti_allowance']}.",
       styles['LeaseBody'])

    _p(story,
       f"6.2 <b>Expansion Option.</b> Expansion option: {d['expansion_option']}. "
       f"Tenant shall have the right to expand into approximately "
       f"{d['expansion_space_sqft']:,} additional square feet of contiguous space, "
       f"exercisable by providing {d['expansion_notice_months']} months' prior "
       f"written notice, no later than month {d['expansion_deadline_month']} of "
       f"the Lease Term. Rent for the expansion space shall be at "
       f"{d['expansion_rent_rate']}. Expansion TI allowance: {d['expansion_ti_allowance']}.",
       styles['LeaseBody'])

    _p(story,
       f"6.3 <b>Right of First Offer.</b> ROFO: {d['rofo_right']}. "
       f"The ROFO shall apply to {d['rofo_space_description']}. Landlord shall "
       f"notify Tenant within {d['rofo_notice_days']} days of any space becoming "
       f"available, and Tenant shall have {d['rofo_response_days']} days to "
       f"respond. Matching terms: {d['rofo_matching_terms']}.",
       styles['LeaseBody'])

    _p(story,
       f"6.4 <b>Right of First Refusal.</b> ROFR: {d['rofr_right']}. "
       f"The ROFR shall apply to {d['rofr_space_description']}. Landlord shall "
       f"provide notice within {d['rofr_notice_days']} days of receiving a bona "
       f"fide third-party offer.",
       styles['LeaseBody'])

    _p(story,
       f"6.5 <b>Purchase Option.</b> Purchase option: {d['purchase_option']}. "
       f"The purchase price shall be determined by {d['purchase_option_price_method']}. "
       f"The option may be exercised during {d['purchase_option_exercise_window']}. "
       f"Upon exercise, Tenant shall have {d['purchase_option_due_diligence_days']} "
       f"days for due diligence and {d['purchase_option_closing_days']} days to "
       f"close the transaction.",
       styles['LeaseBody'])

    _p(story,
       f"6.6 <b>Termination Option.</b> Termination option: "
       f"{d['termination_option']}. Tenant may terminate this Lease effective "
       f"as of the end of month {d['termination_option_effective_month']} of the "
       f"Lease Term, upon {d['termination_notice_months']} months' prior written "
       f"notice, subject to payment of a termination fee equal to "
       f"{d['termination_fee_months_rent']} months' then-current Base Rent. The "
       f"termination fee shall include unamortized TI costs: "
       f"{d['termination_fee_includes_unamortized_ti']}; and unamortized leasing "
       f"commissions: {d['termination_fee_includes_commission']}.",
       styles['LeaseBody'])

    _p(story,
       f"6.7 <b>Contraction Option.</b> Contraction option: "
       f"{d['contraction_option']}. Tenant may reduce the Premises, provided "
       f"Tenant retains not less than {d['contraction_min_sqft_retained']:,} "
       f"rentable square feet, upon {d['contraction_notice_months']} months' "
       f"prior written notice. Contraction fee: {d['contraction_fee_type']}.",
       styles['LeaseBody'])

    _p(story,
       f"6.8 <b>Relocation.</b> Landlord relocation right: "
       f"{d['relocation_right_landlord']}. Any relocation space shall be "
       f"{d['relocation_comparable_space']}. {d['relocation_cost_responsibility']}.",
       styles['LeaseBody'])

    _p(story,
       f"6.9 <b>Must-Take Space.</b> Must-take obligation: "
       f"{d['must_take_space']}. Tenant shall be required to lease an additional "
       f"{d['must_take_space_sqft']:,} rentable square feet no later than month "
       f"{d['must_take_deadline_month']} of the Lease Term, at {d['must_take_rent_rate']}.",
       styles['LeaseBody'])

    _p(story,
       f"6.10 <b>Subletting and Assignment.</b> Sublease consent: "
       f"{d['sublease_consent_required']}. In the event of a sublease at a "
       f"rental rate exceeding Tenant's rental obligation, {d['sublease_profit_sharing_pct']}% "
       f"of the excess profit shall be payable to Landlord.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 7: INSURANCE AND LIABILITY
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 7: INSURANCE AND LIABILITY", styles['SectionHeader'])

    _p(story,
       f"7.1 <b>Tenant's Insurance.</b> Throughout the Lease Term, Tenant shall "
       f"maintain, at Tenant's sole cost and expense, the following insurance "
       f"coverages: (a) commercial general liability insurance with limits of "
       f"not less than {d['gl_coverage_per_occurrence']} per occurrence and "
       f"{d['gl_coverage_aggregate']} in the aggregate, with a maximum "
       f"deductible of {d['gl_deductible_max']}; (b) property insurance covering "
       f"Tenant's personal property and improvements on a {d['property_insurance_coverage']} "
       f"basis, including coverage for Tenant Improvements: "
       f"{d['property_insurance_includes_ti']}; business personal property at "
       f"{d['property_insurance_business_personal_property']}; "
       f"(c) umbrella/excess liability insurance with limits of not less than "
       f"{d['umbrella_excess_liability']}; (d) workers' compensation insurance at "
       f"{d['workers_comp_coverage']}; (e) automobile liability insurance with "
       f"limits of not less than {d['auto_liability_coverage']}; "
       f"(f) business interruption insurance for a period of "
       f"{d['business_interruption_coverage_months']} months; and "
       f"(g) environmental liability insurance with limits of "
       f"{d['environmental_liability_coverage']}.",
       styles['LeaseBody'])

    _p(story,
       f"7.2 <b>Additional Coverage Requirements.</b> Professional liability: "
       f"{d['professional_liability_required']}. Cyber liability: "
       f"{d['cyber_liability_required']}. Pollution legal liability: "
       f"{d['pollution_legal_liability']}. During any construction of Tenant "
       f"Improvements, builders risk insurance shall be provided by: "
       f"{d['builders_risk_during_ti']}. All policies must be issued by carriers "
       f"with a minimum rating of {d['tenant_insurance_carrier_rating']}. Blanket "
       f"policies: {d['blanket_policy_acceptable']}. Self-insurance: "
       f"{d['self_insurance_permitted']}.",
       styles['LeaseBody'])

    _p(story,
       f"7.3 <b>Landlord as Additional Insured.</b> All Tenant insurance policies "
       f"shall name Landlord as additional insured ({d['landlord_additional_insured']}). "
       f"Landlord's lender shall be named as additional insured as follows: "
       f"{d['landlord_lender_additional_insured']}. Certificates of insurance "
       f"shall be delivered within {d['insurance_certificate_delivery_days']} days "
       f"of the Commencement Date and {d['insurance_renewal_notice_days']} days "
       f"prior to renewal. Insurance coverage shall be reviewed "
       f"{d['insurance_review_frequency']}. Adjustments for inflation: "
       f"{d['insurance_adjustment_for_inflation']}.",
       styles['LeaseBody'])

    _p(story,
       f"7.4 <b>Waiver of Subrogation.</b> The parties agree to a "
       f"{d['waiver_of_subrogation']} waiver of subrogation with respect to "
       f"{d['waiver_of_subrogation_scope']}.",
       styles['LeaseBody'])

    _p(story,
       f"7.5 <b>Indemnification.</b> {d['indemnification_by_tenant']}. "
       f"{d['indemnification_by_landlord']}. The indemnification obligations "
       f"shall survive the expiration or earlier termination of this Lease for "
       f"a period of {d['indemnification_survival_months']} months. "
       f"Indemnification cap: {d['indemnification_cap']}. Mutual waiver of "
       f"consequential damages: {d['mutual_waiver_of_consequential_damages']}.",
       styles['LeaseBody'])

    _p(story,
       f"7.6 <b>Limitation of Liability.</b> Landlord's liability shall be "
       f"limited to {d['landlord_liability_cap']}. Tenant's liability cap: "
       f"{d['tenant_liability_cap']}. {d['hold_harmless_scope']}. In the event "
       f"Tenant's use of the Premises causes an increase in insurance premiums, "
       f"{d['insurance_increase_due_to_tenant_use']}.",
       styles['LeaseBody'])

    _p(story,
       f"7.7 <b>Landlord's Insurance.</b> Landlord shall maintain property "
       f"insurance on the Building on an {d['landlord_property_insurance_type']} "
       f"basis, with a deductible not to exceed "
       f"{d['landlord_property_insurance_deductible']}. Additional coverages: "
       f"earthquake insurance ({d['earthquake_insurance']}); flood insurance "
       f"({d['flood_insurance']}); terrorism insurance ({d['terrorism_insurance']}).",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 8: CONSTRUCTION AND ALTERATIONS
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 8: CONSTRUCTION AND ALTERATIONS", styles['SectionHeader'])

    _p(story,
       f"8.1 <b>Initial Build-Out.</b> The initial build-out of the Premises "
       f"shall be the responsibility of {d['initial_buildout_responsibility']} "
       f"and shall be completed within {d['initial_buildout_deadline_days']} days "
       f"of the Commencement Date. Tenant shall submit construction plans for "
       f"Landlord's review within {d['buildout_plan_approval_days']} days; if "
       f"revisions are required, Tenant shall resubmit within "
       f"{d['buildout_plan_resubmission_days']} days. The construction manager "
       f"shall be {d['construction_manager']}. Landlord shall charge a "
       f"construction oversight fee of {d['construction_oversight_fee_pct']}% of "
       f"hard construction costs. Prevailing wage: {d['prevailing_wage_required']}. "
       f"Insurance requirements during construction: "
       f"{d['construction_insurance_requirements']}. Lien waivers: "
       f"{d['construction_lien_waiver_required']}. Completion guarantee: "
       f"{d['construction_completion_guarantee']}.",
       styles['LeaseBody'])

    _p(story,
       f"8.2 <b>Alterations.</b> Tenant may make non-structural alterations "
       f"costing less than {d['alterations_threshold_no_approval']} without "
       f"Landlord's prior consent. {d['alterations_structural_consent']}. "
       f"Cosmetic alterations: {d['alterations_cosmetic_consent']}. Upon "
       f"expiration of the Lease, {d['alterations_removal_at_expiration']}. "
       f"Restoration obligation: {d['alterations_restoration_obligation']}. "
       f"Restoration deposit required: {d['restoration_deposit_required']}. "
       f"Estimated restoration cost: {d['restoration_cost_estimate']}.",
       styles['LeaseBody'])

    _p(story,
       f"8.3 <b>Signage.</b> Tenant shall have the right to install signage as "
       f"follows: {d['signage_right']}, not to exceed {d['signage_size_max_sqft']} "
       f"square feet in area. {d['signage_approval_required']}. All signage costs "
       f"shall be borne by {d['signage_cost_responsibility']}. Upon expiration, "
       f"{d['signage_removal_at_expiration']}.",
       styles['LeaseBody'])

    _p(story,
       f"8.4 <b>Telecommunications.</b> Tenant shall have access to "
       f"{d['telecom_riser_access']} for telecommunications cabling. "
       f"{d['telecom_provider_choice']}. Rooftop telecommunications equipment: "
       f"{d['telecom_equipment_rooftop']}. Monthly rooftop license fee: "
       f"{d['rooftop_license_fee_monthly']}.",
       styles['LeaseBody'])

    _p(story,
       f"8.5 <b>Equipment and Installations.</b> Emergency generator: "
       f"{d['generator_permitted']}, fuel type: {d['generator_fuel_type']}. "
       f"{d['generator_noise_restrictions']}. Solar panels: "
       f"{d['solar_panel_permitted']}. EV charging stations: "
       f"{d['ev_charging_stations_permitted']}. Warehouse racking systems: "
       f"{d['racking_system_approval']}. Floor load capacity: "
       f"{d['floor_load_capacity_psf']} PSF. Mezzanine: "
       f"{d['mezzanine_permitted']}. Hazardous materials storage modifications: "
       f"{d['hazmat_storage_modifications']}.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 9: DEFAULT AND REMEDIES
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 9: DEFAULT AND REMEDIES", styles['SectionHeader'])

    _p(story,
       f"9.1 <b>Events of Default.</b> The occurrence of any of the following "
       f"shall constitute an event of default under this Lease: (a) Tenant's "
       f"failure to pay any installment of Rent within "
       f"{d['monetary_default_cure_days']} days after written notice from Landlord; "
       f"(b) Tenant's failure to perform any non-monetary obligation within "
       f"{d['non_monetary_default_cure_days']} days after written notice from "
       f"Landlord; provided, however, that if such default cannot reasonably be "
       f"cured within such period, Tenant shall have the following additional "
       f"cure period: {d['non_monetary_extended_cure']}.",
       styles['LeaseBody'])

    _p(story,
       f"9.2 <b>Notices.</b> All notices of default shall be delivered by "
       f"{d['notice_of_default_method']}. Notices to Tenant shall be sent to "
       f"{d['notice_of_default_address_tenant']}. Notices to Landlord shall be "
       f"sent to {d['notice_of_default_address_landlord']}.",
       styles['LeaseBody'])

    _p(story,
       f"9.3 <b>Late Charges and Interest.</b> A late charge of "
       f"{d['late_fee_percentage']}% shall apply to any payment not received "
       f"within {d['late_fee_grace_period_days']} days of the due date. Past due "
       f"amounts shall accrue interest at {d['interest_on_past_due_rate']}% per "
       f"annum, calculated by {d['interest_calculation_method']}.",
       styles['LeaseBody'])

    _p(story,
       f"9.4 <b>Landlord's Remedies.</b> Landlord lien on Tenant's property: "
       f"{d['landlord_lien_on_property']}. Lockout right: "
       f"{d['landlord_lockout_right']}. Self-help right: "
       f"{d['landlord_self_help_right']}. Cross-default with affiliated leases: "
       f"{d['cross_default_provision']}; cross-default cure period: "
       f"{d['cross_default_cure_period_days']} days. "
       f"{d['acceleration_of_rent']}. {d['mitigation_of_damages']}. "
       f"Consequential damages: {d['consequential_damages_waiver']}. "
       f"Attorneys' fees shall be awarded to the prevailing party: "
       f"{d['attorneys_fees_prevailing_party']}; cap on recoverable fees: "
       f"{d['attorneys_fees_cap']}.",
       styles['LeaseBody'])

    _p(story,
       f"9.5 <b>Personal Guarantee.</b> This Lease is personally guaranteed by "
       f"{d['guarantor_name']}, {d['guarantor_relationship']} of Tenant. "
       f"Guarantee type: {d['guarantee_type']}. Maximum guarantee amount: "
       f"{d['guarantee_amount_cap']}. Burndown schedule: "
       f"{d['guarantee_burndown_schedule']}. Guarantor shall provide financial "
       f"statements: {d['guarantee_financial_reporting']}.",
       styles['LeaseBody'])

    _p(story,
       f"9.6 <b>Bankruptcy.</b> {d['bankruptcy_provision']}. In the event of "
       f"bankruptcy filing, Tenant shall provide adequate assurance of future "
       f"performance within {d['bankruptcy_adequate_assurance_days']} days.",
       styles['LeaseBody'])

    _p(story,
       f"9.7 <b>Third-Party Cure Rights.</b> Right to cure by Landlord's lender: "
       f"{d['right_to_cure_by_lender']}. Right to cure by Guarantor: "
       f"{d['right_to_cure_by_guarantor']}.",
       styles['LeaseBody'])

    _p(story,
       f"9.8 <b>Surrender.</b> Upon expiration or earlier termination of this "
       f"Lease, Tenant shall surrender the Premises in {d['surrender_condition']} "
       f"condition. Landlord shall have the right to inspect the Premises not "
       f"less than {d['surrender_inspection_days_before']} days prior to the "
       f"Expiration Date. {d['holdover_provision']}. Holdover notice to vacate: "
       f"{d['holdover_notice_to_vacate_days']} days.",
       styles['LeaseBody'])

    _p(story,
       f"9.9 <b>Landlord Default.</b> Landlord shall be in default if Landlord "
       f"fails to perform any obligation within {d['landlord_default_notice_days']} "
       f"days after written notice from Tenant, or within "
       f"{d['landlord_default_cure_days']} days if the default requires extended "
       f"cure. Rent abatement for Landlord default: "
       f"{d['rent_abatement_for_landlord_default']}. Tenant offset right: "
       f"{d['tenant_offset_right']}. Force majeure rent abatement: "
       f"{d['force_majeure_rent_abatement']}. Dispute resolution: "
       f"{d['dispute_resolution_method']}.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 10: ENVIRONMENTAL AND COMPLIANCE
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 10: ENVIRONMENTAL AND COMPLIANCE", styles['SectionHeader'])

    _p(story,
       f"10.1 <b>Hazardous Materials.</b> Tenant's use of hazardous materials: "
       f"{d['hazmat_permitted']}. Permitted hazardous materials include "
       f"{d['hazmat_types_permitted']}. All hazardous materials shall be stored "
       f"in compliance with the following requirements: "
       f"{d['hazmat_storage_requirements']}. Tenant shall report hazardous "
       f"materials usage to Landlord on a {d['hazmat_reporting_frequency']} basis. "
       f"Upon expiration of the Lease, {d['hazmat_removal_at_expiration']}.",
       styles['LeaseBody'])

    _p(story,
       f"10.2 <b>Environmental Assessments.</b> A Phase I Environmental Site "
       f"Assessment was completed on {d['phase_i_esa_date']}: "
       f"{d['phase_i_esa_baseline']}. Phase II ESA: {d['phase_ii_esa_required']}.",
       styles['LeaseBody'])

    _p(story,
       f"10.3 <b>Environmental Indemnification.</b> "
       f"{d['environmental_indemnification_by_tenant']}. "
       f"{d['environmental_indemnification_by_landlord']}. Environmental "
       f"indemnification obligations shall survive for "
       f"{d['environmental_indemnification_survival_years']} years following "
       f"lease expiration. Remediation responsibility: "
       f"{d['environmental_remediation_responsibility']}. Remediation standard: "
       f"{d['environmental_remediation_standard']}. Environmental insurance "
       f"required: {d['environmental_insurance_required']}.",
       styles['LeaseBody'])

    _p(story,
       f"10.4 <b>Building Conditions.</b> Asbestos: "
       f"{d['asbestos_survey_completed']}. Lead paint: "
       f"{d['lead_paint_disclosure']}. {d['mold_prevention_responsibility']}. "
       f"Indoor air quality: {d['indoor_air_quality_standards']}.",
       styles['LeaseBody'])

    _p(story,
       f"10.5 <b>Environmental Compliance.</b> "
       f"{d['stormwater_management_compliance']}. SPCC plan: "
       f"{d['spcc_plan_required']}.",
       styles['LeaseBody'])

    _p(story,
       f"10.6 <b>ADA and Building Codes.</b> ADA compliance: "
       f"{d['ada_compliance_responsibility']}. ADA cost sharing: "
       f"{d['ada_compliance_cost_sharing']}. {d['fire_code_compliance']}. "
       f"Fire sprinkler system: {d['fire_sprinkler_system']}. Fire alarm "
       f"monitoring: {d['fire_alarm_monitoring']}. "
       f"{d['zoning_compliance_warranty_landlord']}. Current zoning: "
       f"{d['zoning_current_classification']}. Special use permit: "
       f"{d['zoning_special_use_permit']}. {d['building_code_compliance']}. "
       f"Energy code: {d['energy_code_compliance']}.",
       styles['LeaseBody'])

    _p(story,
       f"10.7 <b>Operations and Sustainability.</b> Sustainability requirements: "
       f"{d['sustainability_requirements']}. Noise restrictions: "
       f"{d['noise_restrictions']}. Operating hours: "
       f"{d['operating_hours_restrictions']}. Truck traffic: "
       f"{d['truck_traffic_restrictions']}. Odor restrictions: "
       f"{d['odor_emission_restrictions']}.",
       styles['LeaseBody'])
    _sp(story)

    # ══════════════════════════════════════════════════════════════════════
    # ARTICLE 11: MISCELLANEOUS
    # ══════════════════════════════════════════════════════════════════════
    _p(story, "ARTICLE 11: MISCELLANEOUS PROVISIONS", styles['SectionHeader'])

    _p(story,
       f"11.1 <b>Governing Law and Venue.</b> This Lease shall be governed by "
       f"and construed in accordance with the laws of the State of "
       f"{d['governing_law_state']}. Any action arising under this Lease shall "
       f"be brought exclusively in {d['jurisdiction_venue']}. Waiver of jury "
       f"trial: {d['waiver_of_jury_trial']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.2 <b>Force Majeure.</b> Neither party shall be liable for failure "
       f"to perform its obligations (other than the payment of Rent, except as "
       f"otherwise provided) to the extent such failure is caused by "
       f"{d['force_majeure_definition']}. The maximum extension for force majeure "
       f"events shall be {d['force_majeure_max_days']} days. Rent obligation "
       f"during force majeure: {d['force_majeure_rent_obligation']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.3 <b>Subordination and Non-Disturbance.</b> This Lease shall be "
       f"subordinate to any mortgage or deed of trust now or hereafter placed "
       f"upon the Premises ({d['subordination_required']}). "
       f"{d['subordination_non_disturbance']}. SNDA form: {d['snda_form']}. "
       f"{d['attornment_obligation']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.4 <b>Estoppel Certificates.</b> Each party agrees, within "
       f"{d['estoppel_certificate_delivery_days']} days after request by the "
       f"other party, to execute and deliver an estoppel certificate. Frequency: "
       f"{d['estoppel_certificate_frequency']}. The estoppel shall confirm "
       f"{d['estoppel_certificate_content']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.5 <b>Recording.</b> {d['recording_of_lease']}. Recording costs "
       f"shall be borne by {d['recording_cost_responsibility']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.6 <b>Brokers.</b> Landlord represents that it has been represented "
       f"by {d['broker_landlord']} in connection with this Lease. Tenant "
       f"represents that it has been represented by {d['broker_tenant']}. "
       f"{d['broker_commission_responsibility']}. Commission on renewal: "
       f"{d['broker_commission_on_renewal']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.7 <b>Quiet Enjoyment.</b> {d['quiet_enjoyment_covenant']}. "
       f"Landlord access to premises: {d['access_by_landlord']}. Access hours: "
       f"{d['landlord_access_hours']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.8 <b>Parking and Directory.</b> Tenant shall be allocated the "
       f"following parking: {d['parking_allocation']}. Building directory listing: "
       f"{d['signage_on_building_directory']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.9 <b>Assignment.</b> Assignment consent: "
       f"{d['assignment_consent_required']}. Release of assignor: "
       f"{d['assignment_release_of_assignor']}. Transfer fee: "
       f"{d['transfer_fee']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.10 <b>Financial Reporting.</b> Tenant shall provide Landlord with "
       f"financial statements: {d['tenant_financial_reporting']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.11 <b>Notices.</b> All notices under this Lease shall be delivered "
       f"by {d['notices_delivery_method']}. Notices shall be deemed received "
       f"{d['notices_deemed_received']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.12 <b>General Provisions.</b> Confidentiality: "
       f"{d['confidentiality_of_lease_terms']}. {d['entire_agreement_clause']}. "
       f"Amendments: {d['amendment_requirements']}. Severability: "
       f"{d['severability_clause']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.13 <b>Landlord's Lender.</b> Landlord's current mortgage lender is "
       f"{d['landlord_lender_name']}.",
       styles['LeaseBody'])

    _p(story,
       f"11.14 <b>Exhibits.</b> The following exhibits are attached hereto and "
       f"incorporated herein by reference: {d['exhibit_list']}.",
       styles['LeaseBody'])

    # ── Signature Block ──
    _sp(story, 0.3)
    _p(story,
       "IN WITNESS WHEREOF, the parties hereto have executed this Industrial "
       "Lease Agreement as of the date first written above.",
       styles['LeaseBody'])
    _sp(story, 0.3)
    _p(story, f"<b>LANDLORD:</b> {d['landlord_name']}", styles['LeaseBody'])
    _p(story, "By: ________________________&nbsp;&nbsp;&nbsp;Date: ____________",
       styles['LeaseBody'])
    _sp(story, 0.2)
    _p(story, f"<b>TENANT:</b> {d['tenant_name']}", styles['LeaseBody'])
    _p(story, "By: ________________________&nbsp;&nbsp;&nbsp;Date: ____________",
       styles['LeaseBody'])

    doc.build(story)
    return filepath


def generate_complex_lease(num):
    """Generate one complex lease PDF with 351 fields."""
    core = gen_core_terms()
    financial = gen_financial(core)
    opex = gen_operating_expenses(core)
    options = gen_options(core)
    insurance = gen_insurance_liability()
    construction = gen_construction_alterations(core)
    default = gen_default_remedies()
    environmental = gen_environmental_compliance()
    miscellaneous = gen_miscellaneous(core)

    # Merge all sections (remove internal helper keys)
    data = {}
    for section in [core, financial, opex, options, insurance,
                    construction, default, environmental, miscellaneous]:
        for k, v in section.items():
            if not k.startswith('_'):
                data[k] = v

    filename = f"complex_lease_{num:03d}.pdf"
    filepath = build_lease_pdf(data, filename)

    return filename, len(data)


def main():
    print("=" * 60)
    print("Complex Lease Generator — Phase 2")
    print("=" * 60)

    random.seed(42)  # Reproducible output

    total_fields = 0
    for i in range(1, 4):  # Generate 3 complex leases
        filename, field_count = generate_complex_lease(i)
        total_fields = field_count  # Same schema, same count
        print(f"  Generated: {filename} ({field_count} fields)")

    print(f"\nTotal extractable fields per lease: {total_fields}")
    print(f"AI_EXTRACT limit: 100 questions/call")
    print(f"Minimum passes needed: {(total_fields + 99) // 100}")
    print(f"\nOutput directory: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == '__main__':
    main()
