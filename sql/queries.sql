
-- Nombre total de décisions
SELECT COUNT(*) AS total_decisions
FROM dsa_decisions;



-- 2. RÉPARTITION DES DÉCISIONS PAR PLATEFORME

SELECT platform_name, COUNT(*) AS total
FROM dsa_decisions
GROUP BY platform_name
ORDER BY total DESC;



-- 3. TAUX DE MODÉRATION AUTOMATIQUE (SUJET CENTRAL)

-- Pourcentage global automatisé vs non automatisé
SELECT
  automated_decision,
  COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dsa_decisions) AS pourcentage
FROM dsa_decisions
GROUP BY automated_decision;

-- Automatisation par plateforme
SELECT
  platform_name,
  automated_decision,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY platform_name, automated_decision
ORDER BY platform_name, total DESC;



-- 4. ANALYSE TEMPORELLE (VITESSE & ÉVOLUTION)

-- Décisions par jour
SELECT
  application_date,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY application_date
ORDER BY application_date;

-- Décisions par mois
SELECT
  substr(application_date, 4, 7) AS mois,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY mois
ORDER BY mois;



-- 5. TYPES DE DÉCISIONS (REMOVED / DISABLED)

SELECT
  decision_visibility,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY decision_visibility
ORDER BY total DESC;



-- 6. CATÉGORIES JURIDIQUES

SELECT
  category,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY category
ORDER BY total DESC;



-- 7. TYPES DE CONTENUS (PRODUCT, VIDEO, POST…)

SELECT
  content_type,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY content_type
ORDER BY total DESC;



-- 8. LANGUES DES CONTENUS MODÉRÉS

SELECT
  content_language,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY content_language
ORDER BY total DESC;



-- 9. DIMENSION GÉOGRAPHIQUE (BRUT)

SELECT
  territorial_scope,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY territorial_scope
ORDER BY total DESC;




-- 10. COMPARAISON ENTRE PLATEFORMES


SELECT
  platform_name,
  COUNT(*) AS total
FROM dsa_decisions
WHERE platform_name IN ('Google Shopping', 'Shopify')
GROUP BY platform_name;




-- 11. DÉCISIONS LES PLUS RÉCENTES


SELECT *
FROM dsa_decisions
ORDER BY created_at DESC
LIMIT 10;




-- 12. DÉTECTION AUTOMATIQUE VS HUMAINE


SELECT
  automated_detection,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY automated_detection;




-- 13. ACTIVITÉ PAR PLATEFORME ET PAR JOUR


SELECT
  platform_name,
  application_date,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY platform_name, application_date
ORDER BY application_date;




-- 14. REQUÊTE DE SYNTHÈSE GLOBALE (POUR LE RAPPORT)


SELECT
  platform_name,
  COUNT(*) AS total_decisions,
  SUM(CASE WHEN automated_decision = 'FULLY' THEN 1 ELSE 0 END) AS decisions_automatiques,
  SUM(CASE WHEN decision_visibility = 'REMOVED' THEN 1 ELSE 0 END) AS contenus_supprimes
FROM dsa_decisions
GROUP BY platform_name;




-- 15. REQUÊTES POUR L’ANALYSE DES 3V


-- VOLUME
SELECT COUNT(*) AS volume_total
FROM dsa_decisions;

-- VARIÉTÉ : nombre de plateformes différentes
SELECT COUNT(DISTINCT platform_name) AS nb_plateformes
FROM dsa_decisions;

-- VITESSE : croissance par jour
SELECT
  application_date,
  COUNT(*) AS total
FROM dsa_decisions
GROUP BY application_date
ORDER BY application_date;
