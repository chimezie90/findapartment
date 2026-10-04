"""What a Copenhagen-area home costs to buy and to own: one place for the math.

Everything here is an ESTIMATE for a single buyer with a standard Danish
financing setup. Assumptions and sources are in the research page
https://claude.ai/artifact/Dy3xPwdNCLTJWofY5tN4VJ (October 2026):

Financing
- 5% cash down payment (the legal minimum).
- 80% of the price as a realkredit loan: 30-year fixed, 4% coupon, bond
  price 95. You receive 95 per 100 of face value, so the face value (the
  debt) is 0.80 x price / 0.95. Danish realkredit loans pay quarterly, so
  the annuity is computed per quarter.
- Realkredit contribution fee (bidrag): 0.74% a year of the face value
  (Totalkredit, blended 0-80% LTV, fixed rate).
- 15% of the price as a bank loan at 3.70%, 30-year monthly annuity
  (Nykredit BoligLån, 1 Oct 2026).
- Payments are year-1 averages: the year's interest and principal / 12.

Tax deduction (rentefradrag)
- About 33% of the first 50,000 DKK a year of interest + contribution fees,
  about 25% above that.

Running costs
- The listing's monthly owner expenses (ejerudgift) when known.
- Otherwise property taxes estimated from the price, as a new buyer (no
  rebate): value tax 0.51% of 80% of the price (1.4% on the part of that
  80% above 9.007M DKK), plus land tax about 5.3 per mille of 80% of an
  assumed land share of 62% of the price.
  Taxes really follow the official valuation, which may be below the price.

Cash needed at purchase
- 5% down + deed registration (0.6% of the price + 1,850 DKK) + mortgage
  registration (1.25% of both loans' face values) + about 45,000 DKK of
  other fees (loan fees, bank setup, lawyer). About 7.5% of the price.
"""

from typing import Dict, Optional

RESEARCH_URL = "https://claude.ai/artifact/Dy3xPwdNCLTJWofY5tN4VJ"

DOWN_PAYMENT_SHARE = 0.05
REALKREDIT_SHARE = 0.80
REALKREDIT_BOND_PRICE = 0.95
REALKREDIT_COUPON = 0.04
REALKREDIT_YEARS = 30
REALKREDIT_PAYMENTS_PER_YEAR = 4
CONTRIBUTION_FEE = 0.0074
BANK_LOAN_SHARE = 0.15
BANK_LOAN_RATE = 0.037
BANK_LOAN_YEARS = 30

TAX_DEDUCTION_LOW_RATE = 0.33
TAX_DEDUCTION_HIGH_RATE = 0.25
TAX_DEDUCTION_THRESHOLD = 50_000  # DKK of interest + fees per year

VALUE_TAX_BASE_SHARE = 0.80
VALUE_TAX_RATE = 0.0051
VALUE_TAX_HIGH_RATE = 0.014
VALUE_TAX_HIGH_THRESHOLD = 9_007_000  # on the 80% base, 2026
LAND_TAX_RATE = 0.0053  # Copenhagen, per year
LAND_SHARE_OF_PRICE = 0.62

DEED_FEE_RATE = 0.006
DEED_FEE_FIXED = 1_850
MORTGAGE_REGISTRATION_RATE = 0.0125
OTHER_PURCHASE_FEES = 45_000

SQFT_PER_SQM = 10.7639

# Average asking/sales price per m2 in DKK, Q2 2026, by municipality:
# (flats, houses). Source: Finans Danmark, Boligmarkedsstatistikken, Q2 2026,
# via the research page above. Terraced houses compare with houses; villa
# flats (villalejligheder) are owner-occupied flats.
MUNICIPALITY_AVG_PRICE_PER_SQM: Dict[str, Dict[str, int]] = {
    "København": {"flat": 75_120, "house": 65_642},
    "Frederiksberg": {"flat": 83_530, "house": 87_689},
    "Gentofte": {"flat": 65_871, "house": 70_117},
    "Lyngby-Taarbæk": {"flat": 51_216, "house": 60_638},
    "Rødovre": {"flat": 49_462, "house": 47_955},
    "Gladsaxe": {"flat": 46_632, "house": 49_717},
    "Hvidovre": {"flat": 41_716, "house": 45_234},
    "Ballerup": {"flat": 41_339, "house": 38_949},
}
_AVERAGE_CLASS = {"flat": "flat", "villa_flat": "flat", "villa": "house", "terraced": "house"}


def _annuity_year1(principal: float, annual_rate: float, years: int, per_year: int) -> Dict[str, float]:
    """Year-1 totals of an annuity loan: payment, interest, principal (DKK/year)."""
    rate = annual_rate / per_year
    periods = years * per_year
    payment = principal * rate / (1 - (1 + rate) ** -periods)
    balance, interest = principal, 0.0
    for _ in range(per_year):
        period_interest = balance * rate
        interest += period_interest
        balance -= payment - period_interest
    total = payment * per_year
    return {"payment": total, "interest": interest, "principal": total - interest}


# When a listing's owner expenses (ejerudgift) are unknown: owners'-association
# fees and insurance on top of the estimated property tax, so every listing's
# monthly cost covers the same things (DKK/month, rough typical values)
OWNER_COSTS_ALLOWANCE = {"flat": 2750, "villa_flat": 2000, "terraced": 1500, "villa": 800}


def realkredit_face_value(price: float) -> float:
    """Debt taken on to receive 80% of the price at bond price 95."""
    return REALKREDIT_SHARE * price / REALKREDIT_BOND_PRICE


def estimated_property_tax_monthly(price: float) -> float:
    """Value tax + land tax for a new buyer, assuming valuation = price (DKK/month)."""
    base = VALUE_TAX_BASE_SHARE * price
    value_tax = VALUE_TAX_RATE * min(base, VALUE_TAX_HIGH_THRESHOLD)
    value_tax += VALUE_TAX_HIGH_RATE * max(base - VALUE_TAX_HIGH_THRESHOLD, 0)
    land_tax = LAND_TAX_RATE * VALUE_TAX_BASE_SHARE * LAND_SHARE_OF_PRICE * price
    return (value_tax + land_tax) / 12


def estimate_monthly_cost(price_dkk: float, monthly_owner_expenses_dkk: Optional[float] = None,
                          property_type: Optional[str] = None) -> Dict[str, object]:
    """
    Estimated monthly cost of owning a home bought at `price_dkk` (year 1).

    Returns DKK per month, rounded:
        gross_loan_payment: realkredit + contribution fee + bank loan, before tax
        tax_deduction: value of the interest deduction
        net_loan_payment: gross_loan_payment - tax_deduction
        principal: repayment included in the loan payments (equity you keep)
        running_costs: owner expenses, or estimated property taxes
        running_costs_estimated: True if running_costs is the tax estimate
        cash_out: net_loan_payment + running_costs (what leaves your account)
        real_cost: cash_out - principal (the economic cost)
    """
    price = float(price_dkk)
    face = realkredit_face_value(price)
    realkredit = _annuity_year1(face, REALKREDIT_COUPON, REALKREDIT_YEARS, REALKREDIT_PAYMENTS_PER_YEAR)
    bank = _annuity_year1(BANK_LOAN_SHARE * price, BANK_LOAN_RATE, BANK_LOAN_YEARS, 12)
    fee = CONTRIBUTION_FEE * face

    deductible = realkredit["interest"] + fee + bank["interest"]
    deduction = (TAX_DEDUCTION_LOW_RATE * min(deductible, TAX_DEDUCTION_THRESHOLD)
                 + TAX_DEDUCTION_HIGH_RATE * max(deductible - TAX_DEDUCTION_THRESHOLD, 0))

    gross = (realkredit["payment"] + fee + bank["payment"]) / 12
    net = gross - deduction / 12
    principal = (realkredit["principal"] + bank["principal"]) / 12
    estimated = not monthly_owner_expenses_dkk
    running = (estimated_property_tax_monthly(price) + OWNER_COSTS_ALLOWANCE.get(property_type, 0)
               if estimated else float(monthly_owner_expenses_dkk))
    cash_out = net + running
    return {
        "gross_loan_payment": round(gross),
        "tax_deduction": round(deduction / 12),
        "net_loan_payment": round(net),
        "principal": round(principal),
        "running_costs": round(running),
        "running_costs_estimated": estimated,
        "cash_out": round(cash_out),
        "real_cost": round(cash_out - principal),
    }


def cash_needed(price_dkk: float) -> int:
    """Cash needed on purchase day: down payment plus purchase fees (DKK)."""
    price = float(price_dkk)
    loan_face_values = realkredit_face_value(price) + BANK_LOAN_SHARE * price
    return round(
        DOWN_PAYMENT_SHARE * price
        + DEED_FEE_RATE * price + DEED_FEE_FIXED
        + MORTGAGE_REGISTRATION_RATE * loan_face_values
        + OTHER_PURCHASE_FEES
    )


def municipality_average_per_sqm(municipality: Optional[str], property_type: str) -> Optional[int]:
    """Q2 2026 average DKK/m2 for this municipality and property class, if known."""
    averages = MUNICIPALITY_AVG_PRICE_PER_SQM.get(municipality or "")
    cls = _AVERAGE_CLASS.get(property_type)
    return averages.get(cls) if averages and cls else None


def assumptions() -> Dict[str, object]:
    """The assumptions, for the UI's details panel."""
    return {
        "down_payment_pct": DOWN_PAYMENT_SHARE * 100,
        "realkredit_pct": REALKREDIT_SHARE * 100,
        "realkredit_coupon_pct": REALKREDIT_COUPON * 100,
        "realkredit_bond_price": REALKREDIT_BOND_PRICE * 100,
        "realkredit_years": REALKREDIT_YEARS,
        "contribution_fee_pct": CONTRIBUTION_FEE * 100,
        "bank_loan_pct": BANK_LOAN_SHARE * 100,
        "bank_loan_rate_pct": BANK_LOAN_RATE * 100,
        "bank_loan_years": BANK_LOAN_YEARS,
        "tax_deduction_low_pct": TAX_DEDUCTION_LOW_RATE * 100,
        "tax_deduction_high_pct": TAX_DEDUCTION_HIGH_RATE * 100,
        "tax_deduction_threshold_dkk": TAX_DEDUCTION_THRESHOLD,
        "research_url": RESEARCH_URL,
    }
