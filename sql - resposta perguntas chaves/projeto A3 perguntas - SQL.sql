1. Número de indústrias ativas por mês/ano entre 2010 - 2021, discriminado por MEI
ou Simples, em cada município brasileiro

#_______________________________________________________________________________________________
# 
select 
    EXTRACT(YEAR from a.dt_inicio_atividade) as ano,
    EXTRACT(MONTH from a.dt_inicio_atividade) as mes,
    COUNT(a.cnpj_basico) as qtd_empresas, 
    d.nom_municipio, b.idc_op_mei, b.idc_op_simples
from receita_cnpj.estabelecimento as a
    inner join receita_cnpj.simples as b
        on a.cnpj_basico = b.cnpj_basico
    left join receita_cnpj.pais as c
        on a.cod_pais = c.cod_pais
    left join receita_cnpj.municipio as d
        on a.cod_municipio = d.cod_municipio
where EXTRACT(YEAR from a.dt_inicio_atividade) between 2010 and 2021 and uf <> 'EX'
    and a.cod_cnae_principal >= 500000 and a.cod_cnae_principal < 3400000 /*filtro referente à categoria 'Indústria' levando em consideração o código da categoria na CNAE*/
    and (idc_op_mei = 'S' or idc_op_simples = 'S')
group by 
    ano,
    mes,
    d.nom_municipio,
    b.idc_op_mei, 
    b.idc_op_simples;
#_______________________________________________________________________________________________
# 

2. Número de comércios que fecharam por mês/ano entre 2010 - 2021, discriminado
por MEI ou Simples, em cada município brasileiro

#_______________________________________________________________________________________________
# 
select 
    EXTRACT(YEAR from a.dt_inicio_atividade) as ano,
    EXTRACT(MONTH from a.dt_inicio_atividade) as mes,
    COUNT(DISTINCT a.cnpj_basico) as qtd_empresas, 
    d.nom_municipio, b.idc_op_mei, b.idc_op_simples
from receita_cnpj.estabelecimento as a
    inner join receita_cnpj.simples as b
        on a.cnpj_basico = b.cnpj_basico
    left join receita_cnpj.pais as c
        on a.cod_pais = c.cod_pais
    left join receita_cnpj.municipio as d
        on a.cod_municipio = d.cod_municipio
where EXTRACT(YEAR from a.dt_inicio_atividade) between 2010 and 2021 and uf <> 'EX'
    and a.cod_cnae_principal >= 4500000 and a.cod_cnae_principal < 4800000 /*filtro referente à categoria 'Comércio' levando em consideração o código da categoria na CNAE*/
    and (idc_op_mei = 'S' or idc_op_simples = 'S')
group by ano, mes, d.nom_municipio,
    b.idc_op_mei, b.idc_op_simples;
#_______________________________________________________________________________________________
# 

3. Número de CNPJ novos por mês/ano entre 2010 - 2021, discriminado por MEI ou
Simples, em cada município brasileiro

#_______________________________________________________________________________________________
# 
select 
    EXTRACT(YEAR from a.dt_inicio_atividade) as ano,
    EXTRACT(MONTH from a.dt_inicio_atividade) as mes,
    COUNT(a.cnpj_basico) as qtd_empresas, 
    d.nom_municipio, b.idc_op_mei, b.idc_op_simples
from receita_cnpj.estabelecimento as a
    inner join receita_cnpj.simples as b
        on a.cnpj_basico = b.cnpj_basico
    left join receita_cnpj.pais as c
        on a.cod_pais = c.cod_pais
    left join receita_cnpj.municipio as d
        on a.cod_municipio = d.cod_municipio
where EXTRACT(YEAR from a.dt_inicio_atividade) between 2010 and 2021 and uf <> 'EX'
    and (idc_op_mei = 'S' or idc_op_simples = 'S')
group by ano, mes, d.nom_municipio,
    b.idc_op_mei, b.idc_op_simples;
#_______________________________________________________________________________________________
# 

4. Qual o número de CNPJ que surgiram do grupo de educação superior, entre 2015
e 2021, discriminado por ano, em cada estado brasileiro?

#_______________________________________________________________________________________________
# 
select 
    EXTRACT(YEAR from a.dt_inicio_atividade) as ano,
    a.uf,
    COUNT(DISTINCT a.cnpj_basico) as qtd_empresas
from receita_cnpj.estabelecimento as a
    left join receita_cnpj.pais as c
        on a.cod_pais = c.cod_pais
where EXTRACT(YEAR from a.dt_inicio_atividade) between 2010 and 2021 and uf <> 'EX'
    and a.cod_cnae_principal >= 8530000 and a.cod_cnae_principal < 8540000 /*filtro referente à categoria 'Educação Superior' levando em consideração o código da categoria na CNAE*/
group by ano, a.uf;
#_______________________________________________________________________________________________
# 

5. Qual a classe de CNAE com o maior capital social médio no Brasil no último ano?

#_______________________________________________________________________________________________
# 
select * from 
    (select
        b.nom_cnae, AVG(c.capital_social) as capital_social
    from receita_cnpj.estabelecimento as a
        left join receita_cnpj.cnae as b
            on a.cod_cnae_principal = b.cod_cnae
        left join receita_cnpj.empresa as c
            on a.cnpj_basico = c.cnpj_basico
    where a.uf <> 'EX' and a.dt_inicio_atividade >= DATE_TRUNC(current_date(), YEAR)
    group by b.nom_cnae)
order by 2 desc limit 1
#_______________________________________________________________________________________________
# 

6. Qual a média do capital social das empresas de pequeno porte por natureza
jurídica no último ano?

#_______________________________________________________________________________________________
# 
select 
    em.cod_nat_juridica,
    AVG(em.capital_social) as media_capital_social
from receita_cnpj.estabelecimento as es
    left join receita_cnpj.empresa as em
        on es.cnpj_basico = em.cnpj_basico
    left join receita_cnpj.natureza_juridica as nj
        on em.cod_nat_juridica = nj.cod_nat_juridica
where em.cod_porte = 3 and es.dt_inicio_atividade >= DATE_TRUNC(current_date(), YEAR)
group by em.cod_nat_juridica
#_______________________________________________________________________________________________
# 


