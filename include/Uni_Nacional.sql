SELECT  
    nacional.univiersities                                  AS university
    ,nacional.carrera                                       AS career
    ,nacional.inscription_dates                             AS inscription_date
    ,SPLIT_PART(nacional.names,'-',1)                       AS first_name
    ,REVERSE(SPLIT_PART(REVERSE(nacional.names),'-',1))     AS last_name
    ,nacional.sexo                                          AS gender
    ,nacional.fechas_nacimiento                             AS birth_date
    ,nacional.direcciones                                   AS location
    ,nacional.email                                        AS email
FROM 
    rio_cuarto_interamericana AS nacional
WHERE 
    nacional.univiersities = 'Universidad-nacional-de-río-cuarto'
    AND to_date(inscription_dates, 'DD/MM/YYYY') >= '01/09/2020'
    AND to_date(inscription_dates, 'DD/MM/YYYY') <= '01/02/2021'