SELECT
  overall_rating,
  AVG(severity_weighted_score) AS avg_severity_score,
  COUNT(*) AS facility_count
FROM cms_nh_gold.gold_facility_risk_profile
GROUP BY overall_rating
ORDER BY overall_rating;
