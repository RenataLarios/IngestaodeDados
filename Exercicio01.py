import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_array_to_cols
from awsglue.dynamicframe import DynamicFrame
import gs_split
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node dadosreclamacoes
dadosreclamacoes_node1691457981600 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ";",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://exercicio01ingestaodedados/Dados/Reclamacoes/"],
        "recurse": True,
    },
    transformation_ctx="dadosreclamacoes_node1691457981600",
)

# Script generated for node dadosbancos
dadosbancos_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://exercicio01ingestaodedados/Dados/Bancos/"],
        "recurse": True,
    },
    transformation_ctx="dadosbancos_node1",
)

# Script generated for node dadosempregados
dadosempregados_node1690846955226 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "|",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://exercicio01ingestaodedados/Dados/Empregados/"],
        "recurse": True,
    },
    transformation_ctx="dadosempregados_node1690846955226",
)

# Script generated for node Select Fields - Reclamações
SelectFieldsReclamaes_node1691372799075 = SelectFields.apply(
    frame=dadosreclamacoes_node1691457981600,
    paths=[
        "cnpj if",
        "quantidade total de reclamações",
        "quantidade total de clientes  ccs e scr",
    ],
    transformation_ctx="SelectFieldsReclamaes_node1691372799075",
)

# Script generated for node Split String
SplitString_node1691449040655 = dadosbancos_node1.gs_split(
    colName="Nome", pattern=" - PRUDENCIAL", newColName="bancoarray"
)

# Script generated for node Select Fields - Empregados
SelectFieldsEmpregados_node1691372576395 = SelectFields.apply(
    frame=dadosempregados_node1690846955226,
    paths=["Remuneração e benefícios", "Geral", "CNPJ", "Nome"],
    transformation_ctx="SelectFieldsEmpregados_node1691372576395",
)

# Script generated for node Rename Field
RenameField_node1691456124929 = RenameField.apply(
    frame=SelectFieldsReclamaes_node1691372799075,
    old_name="cnpj if",
    new_name="cnpjif",
    transformation_ctx="RenameField_node1691456124929",
)

# Script generated for node Array To Columns
ArrayToColumns_node1691450582533 = SplitString_node1691449040655.gs_array_to_cols(
    colName="bancoarray", colList="banco", indexes="1"
)

# Script generated for node Select Fields - Bancos
SelectFieldsBancos_node1691451233386 = SelectFields.apply(
    frame=ArrayToColumns_node1691450582533,
    paths=["cnpj", "banco", "segmento"],
    transformation_ctx="SelectFieldsBancos_node1691451233386",
)

# Script generated for node Join
Join_node1691451769797 = Join.apply(
    frame1=SelectFieldsBancos_node1691451233386,
    frame2=RenameField_node1691456124929,
    keys1=["cnpj"],
    keys2=["cnpjif"],
    transformation_ctx="Join_node1691451769797",
)

# Script generated for node Join
Join_node1691452981017 = Join.apply(
    frame1=Join_node1691451769797,
    frame2=SelectFieldsEmpregados_node1691372576395,
    keys1=["banco"],
    keys2=["Nome"],
    transformation_ctx="Join_node1691452981017",
)

# Script generated for node Select Fields
SelectFields_node1691453975150 = SelectFields.apply(
    frame=Join_node1691452981017,
    paths=[
        "cnpj",
        "banco",
        "Geral",
        "Remuneração e benefícios",
        "quantidade total de reclamações",
        "quantidade total de clientes  ccs e scr",
        "segmento",
    ],
    transformation_ctx="SelectFields_node1691453975150",
)

# Script generated for node Aggregate
Aggregate_node1691550674401 = sparkAggregate(
    glueContext,
    parentFrame=SelectFields_node1691453975150,
    groups=["segmento", "cnpj", "banco"],
    aggs=[
        ["quantidade total de reclamações", "sum"],
        ["quantidade total de clientes  ccs e scr", "sum"],
        ["Geral", "avg"],
        ["Remuneração e benefícios", "avg"],
    ],
    transformation_ctx="Aggregate_node1691550674401",
)

# Script generated for node Amazon S3 - Resultado
AmazonS3Resultado_node1691454112950 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1691550674401,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://exercicio01ingestaodedados/Resultado/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3Resultado_node1691454112950",
)

job.commit()
