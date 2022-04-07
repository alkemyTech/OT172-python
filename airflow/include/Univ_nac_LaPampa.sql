--Query to obtain raw data on people enrolled in Universodad de
-- La Pampa


SELECT
universidad as university,
carrerra as career, 
fechaiscripccion as inscription_date,
nombrre as name, 
sexo as gender,
nacimiento, 
codgoposstal as postal_code,
eemail as email  
FROM
moron_nacional_pampa
WHERE
universidad = 'Universidad nacional de la pampa'
AND TO_DATE(fechaiscripccion,'DD/MM/YYYY') >= '01/09/2020'
AND TO_DATE(fechaiscripccion,'DD/MM/YYYY') <  '01/02/2021';

