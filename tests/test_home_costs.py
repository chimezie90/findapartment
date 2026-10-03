"""Monthly cost and cash-needed estimates for homes (pure math, no DB).

Sanity figures for a 7.5M DKK flat come from the research page
(https://claude.ai/artifact/Dy3xPwdNCLTJWofY5tN4VJ): about 39,640 DKK/month
gross loan payment, about 32,100 after tax, about 11,200 of it principal.
Our figures are ~1% lower because the research's bank-loan line (5,360/mo)
is above the true 3.70% 30-year annuity (5,178/mo).
"""

import pytest

from apartment_finder.homes import costs


def test_sanity_numbers_for_a_7_5m_flat():
    est = costs.estimate_monthly_cost(7_500_000)
    assert est["gross_loan_payment"] == pytest.approx(39_600, rel=0.015)
    assert est["net_loan_payment"] == pytest.approx(32_100, rel=0.015)
    assert est["principal"] == pytest.approx(11_200, rel=0.02)
    assert est["tax_deduction"] == pytest.approx(7_500, rel=0.02)
    # Unknown owner expenses: estimated property taxes, ~4,200/month at valuation = price
    assert est["running_costs_estimated"] is True
    assert est["running_costs"] == pytest.approx(4_193, abs=2)
    assert est["cash_out"] == est["net_loan_payment"] + est["running_costs"]
    assert est["real_cost"] == est["cash_out"] - est["principal"]


def test_exact_figures_for_a_7_5m_flat_are_pinned():
    # Hand-computed independently: realkredit face 6,315,789 at 4%/quarterly,
    # bank 1,125,000 at 3.70%/monthly, fee 0.74% of face; deductible interest
    # + fees ~339,000/yr -> 33% of 50,000 + 25% of the rest.
    assert costs.estimate_monthly_cost(7_500_000) == {
        "gross_loan_payment": 39_277,
        "tax_deduction": 7_395,
        "net_loan_payment": 31_882,
        "principal": 11_029,
        "running_costs": 4_193,
        "running_costs_estimated": True,
        "cash_out": 36_075,
        "real_cost": 25_046,
    }


def test_owner_expenses_replace_the_tax_estimate():
    est = costs.estimate_monthly_cost(7_500_000, monthly_owner_expenses_dkk=2_411)
    assert est["running_costs_estimated"] is False
    assert est["running_costs"] == 2_411
    assert est["cash_out"] == est["net_loan_payment"] + 2_411


def test_loan_parts_match_hand_calculations():
    face = costs.realkredit_face_value(7_500_000)
    assert face == pytest.approx(6_315_789, abs=1)
    year1 = costs._annuity_year1(1_125_000, 0.037, 30, 12)
    assert year1["payment"] / 12 == pytest.approx(5_178, abs=1)  # standard annuity formula
    assert year1["interest"] / 12 == pytest.approx(3_440, abs=5)


def test_tax_deduction_drops_to_25_percent_above_50k():
    # Tiny price: all deductible interest under 50,000/yr -> 33%
    small = costs.estimate_monthly_cost(500_000)
    deductible_small = (small["gross_loan_payment"] - small["principal"]) * 12
    assert deductible_small < 50_000
    assert small["tax_deduction"] * 12 == pytest.approx(0.33 * deductible_small, rel=0.01)


def test_value_tax_uses_the_higher_rate_above_the_threshold():
    # 20M: 80% base = 16M, of which 6.993M is above 9.007M
    expected = (0.0051 * 9_007_000 + 0.014 * 6_993_000 + 0.0053 * 0.8 * 0.62 * 20_000_000) / 12
    assert costs.estimated_property_tax_monthly(20_000_000) == pytest.approx(expected)


def test_cash_needed_is_about_7_5_percent():
    cash = costs.cash_needed(7_500_000)
    face_values = 6_315_789.47 + 1_125_000
    assert cash == round(375_000 + 45_000 + 1_850 + 0.0125 * face_values + 45_000)
    assert cash / 7_500_000 == pytest.approx(0.0746, abs=0.001)


def test_municipality_averages_by_property_class():
    assert costs.municipality_average_per_sqm("København", "flat") == 75_120
    assert costs.municipality_average_per_sqm("København", "villa_flat") == 75_120
    assert costs.municipality_average_per_sqm("Frederiksberg", "terraced") == 87_689
    assert costs.municipality_average_per_sqm("Ballerup", "villa") == 38_949
    assert costs.municipality_average_per_sqm("Dragør", "villa") is None
    assert costs.municipality_average_per_sqm(None, "flat") is None
