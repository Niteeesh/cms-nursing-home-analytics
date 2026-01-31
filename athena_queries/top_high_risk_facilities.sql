SELECT
  ccn,
  provider_name,
  state,
  total_citations,
  severity_weighted_score,
  infection_control_rate,
  overall_rating
FROM cms_nh_gold.gold_facility_risk_profile
ORDER BY severity_weighted_score DESC
LIMIT 10;
