SELECT  
    moron.universidad                                   AS university
    ,moron.carrerra                                     AS career
    ,moron.fechaiscripccion                             AS inscription_date
    ,SPLIT_PART(moron.nombrre,' ',1)                    AS first_name
    ,REVERSE(SPLIT_PART(REVERSE(moron.nombrre),' ',1))  AS last_name
    ,moron.sexo                                         AS gender
    ,moron.nacimiento                                   AS age
    ,moron.codgoposstal                                 AS postal_code
    ,moron.eemail                                       AS email
FROM 
    moron_nacional_pampa AS moron
WHERE 
    TRANSLATE(moron.universidad ,'ÁÉÍÓÚáéíóú','AEIOUaeiou') LIKE TRANSLATE('%%Universidad de morón%%','ÁÉÍÓÚáéíóú','AEIOUaeiou')
    AND to_date(fechaiscripccion , 'DD/MM/YYYY') >= '01/09/2020'
    AND to_date(fechaiscripccion , 'DD/MM/YYYY') <= '01/02/2021'