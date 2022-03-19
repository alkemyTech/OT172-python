SELECT
universidad, carrerra, fechaiscripccion, sexo, nombrre, eemail, codgoposstal, nacimiento 
FROM
moron_nacional_pampa
WHERE
universidad = 'Universidad nacional de la pampa'
AND TO_DATE(fechaiscripccion,'DD/MM/YYYY') >= '01/09/2020'
AND TO_DATE(fechaiscripccion,'DD/MM/YYYY') <  '01/02/2021'
