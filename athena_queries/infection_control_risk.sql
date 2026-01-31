SELECT
  ccn,
  provider_name,
  state,
  infection_control_rate,
  total_citations
FROM cms_nh_gold.gold_facility_risk_profile
WHERE total_citations >= 5
ORDER BY infection_control_rate DESC
LIMIT 10;
