SELECT  
    nacional.univiersities                                  AS university
    ,nacional.carrera                                       AS career
    ,nacional.inscription_dates                             AS inscription_date
    ,SPLIT_PART(nacional.names,'-',1)                       AS first_name
    ,REVERSE(SPLIT_PART(REVERSE(nacional.names),'-',1))     AS last_name
    ,nacional.sexo                                          AS gender
    ,nacional.fechas_nacimiento                             AS age
    ,nacional.direcciones                                   AS location
    ,nacional.email                                         AS email
FROM 
    rio_cuarto_interamericana AS nacional
WHERE 
    TRANSLATE(nacional.univiersities,'ÁÉÍÓÚáéíóú','AEIOUaeiou') LIKE TRANSLATE('%%Universidad-nacional-de-río-cuarto%%','ÁÉÍÓÚáéíóú','AEIOUaeiou')
    and(DATE(inscription_dates) BETWEEN '2020-Sep-01' and  '2021-Feb-01')