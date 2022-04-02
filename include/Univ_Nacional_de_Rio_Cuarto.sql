  SELECT  
    nacional.univiersities                                  AS university
    ,nacional.carrera                                       AS career
    ,nacional.inscription_dates                             AS inscription_date
    ,names                                                 AS name
     ,nacional.sexo                                          AS gender
    ,nacional.fechas_nacimiento                             AS nacimiento
    ,nacional.localidad                                     AS location
    ,nacional.email                                         AS email
FROM 
    rio_cuarto_interamericana AS nacional
WHERE 
    TRANSLATE(nacional.univiersities,'ÁÉÍÓÚáéíóú','AEIOUaeiou') LIKE TRANSLATE('%%Universidad-nacional-de-río-cuarto%%','ÁÉÍÓÚáéíóú','AEIOUaeiou')
    and to_date(inscription_dates , 'YY-Mon-DD') 
    BETWEEN to_date('2020-Sep-01', 'YY-Mon-DD') 
        AND to_date('2021-Feb-01', 'YY-Mon-DD')
	order by to_date(inscription_dates , 'YY-Mon-DD') asc