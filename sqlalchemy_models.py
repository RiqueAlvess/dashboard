from sqlalchemy import (
    Column, Integer, String, Date, DateTime, Numeric, Boolean, 
    ForeignKey, UniqueConstraint, Index, Text, Time
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

# =============================================================================
# TABELAS DIMENSÃO
# =============================================================================

class DimEmpresa(Base):
    """Dimensão Empresa - Informações master das empresas"""
    __tablename__ = 'dim_empresa'
    
    # Surrogate Key
    sk_empresa = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural Key
    codigo_empresa = Column(Integer, unique=True, nullable=False, index=True)
    
    # Atributos
    nome_empresa = Column(String(200), nullable=False)
    cnpj = Column(String(20))
    situacao = Column(String(20))
    
    # Metadados de controle
    data_carga = Column(DateTime, default=datetime.utcnow)
    data_atualizacao = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relacionamentos
    funcionarios = relationship("DimFuncionario", back_populates="empresa")
    vencimentos = relationship("FatoVencimento", back_populates="empresa")


class DimUnidade(Base):
    """Dimensão Unidade - Informações das unidades organizacionais"""
    __tablename__ = 'dim_unidade'
    
    # Surrogate Key
    sk_unidade = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural Keys
    codigo_empresa = Column(Integer, ForeignKey('dim_empresa.codigo_empresa'), nullable=False)
    codigo_unidade = Column(String(20), nullable=False)
    
    # Atributos
    nome_unidade = Column(String(130), nullable=False)
    status_unidade = Column(String(20))
    cnpj_unidade = Column(String(20))
    endereco = Column(String(110))
    numero_endereco = Column(String(20))
    bairro = Column(String(80))
    cidade = Column(String(50))
    uf = Column(String(20))
    cep = Column(String(10))
    cnae = Column(String(20))
    cnae_2_0 = Column(String(20))
    cnae_7 = Column(String(20))
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    data_atualizacao = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('codigo_empresa', 'codigo_unidade'),
        Index('idx_unidade_empresa', 'codigo_empresa'),
    )


class DimSetor(Base):
    """Dimensão Setor - Setores organizacionais"""
    __tablename__ = 'dim_setor'
    
    # Surrogate Key
    sk_setor = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural Keys
    codigo_empresa = Column(Integer, nullable=False)
    codigo_setor = Column(String(12), nullable=False)
    
    # Atributos
    nome_setor = Column(String(130), nullable=False)
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('codigo_empresa', 'codigo_setor'),
        Index('idx_setor_empresa', 'codigo_empresa'),
    )


class DimCargo(Base):
    """Dimensão Cargo - Cargos e funções"""
    __tablename__ = 'dim_cargo'
    
    # Surrogate Key
    sk_cargo = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural Keys
    codigo_cargo = Column(String(10), nullable=False)
    
    # Atributos
    nome_cargo = Column(String(130), nullable=False)
    cbo_cargo = Column(String(10))
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('codigo_cargo'),
        Index('idx_cargo_codigo', 'codigo_cargo'),
    )


class DimExame(Base):
    """Dimensão Exame - Tipos de exames ocupacionais"""
    __tablename__ = 'dim_exame'
    
    # Surrogate Key
    sk_exame = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural Key
    codigo_exame = Column(Integer, nullable=False, unique=True, index=True)
    
    # Atributos
    nome_exame = Column(String(200), nullable=False)
    periodicidade = Column(Integer)  # dias
    obrigatorio = Column(Boolean, default=True)
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)


class DimTempo(Base):
    """Dimensão Tempo - Hierarquia temporal para análises"""
    __tablename__ = 'dim_tempo'
    
    # Surrogate Key
    sk_tempo = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural Key
    data_completa = Column(Date, unique=True, nullable=False, index=True)
    
    # Hierarquia temporal
    ano = Column(Integer, nullable=False)
    trimestre = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    dia = Column(Integer, nullable=False)
    dia_semana = Column(Integer, nullable=False)  # 1=Segunda, 7=Domingo
    semana_ano = Column(Integer, nullable=False)
    nome_mes = Column(String(20), nullable=False)
    nome_dia_semana = Column(String(20), nullable=False)
    eh_feriado = Column(Boolean, default=False)
    eh_final_semana = Column(Boolean, default=False)
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    
    # Constraints
    __table_args__ = (
        Index('idx_tempo_ano_mes', 'ano', 'mes'),
        Index('idx_tempo_trimestre', 'ano', 'trimestre'),
    )


class DimFuncionario(Base):
    """Dimensão Funcionário - SCD Tipo 2 para histórico de mudanças"""
    __tablename__ = 'dim_funcionario'
    
    # Surrogate Key
    sk_funcionario = Column(Integer, primary_key=True, autoincrement=True)
    
    # Natural Key
    codigo_funcionario = Column(Integer, nullable=False, index=True)
    codigo_empresa = Column(Integer, ForeignKey('dim_empresa.codigo_empresa'), nullable=False)
    
    # Atributos pessoais
    nome = Column(String(120), nullable=False)
    cpf = Column(String(19), nullable=False, index=True)
    rg = Column(String(19))
    uf_rg = Column(String(10))
    orgao_emissor_rg = Column(String(20))
    data_nascimento = Column(Date, nullable=False)
    sexo = Column(Integer, nullable=False)  # 1=M, 2=F
    estado_civil = Column(Integer)  # 1-7 conforme legenda
    cor = Column(Integer)
    escolaridade = Column(Integer)
    naturalidade = Column(String(50))
    nome_mae = Column(String(120))
    
    # Dados profissionais
    matricula_funcionario = Column(String(30), nullable=False)
    matricula_rh = Column(String(30))
    situacao = Column(String(12), nullable=False)
    data_admissao = Column(Date, nullable=False)
    data_demissao = Column(Date)
    tipo_contratacao = Column(Integer)
    
    # Organização
    codigo_unidade = Column(String(20))
    nome_unidade = Column(String(130))
    codigo_setor = Column(String(12))
    nome_setor = Column(String(130))
    codigo_cargo = Column(String(10))
    nome_cargo = Column(String(130))
    cbo_cargo = Column(String(10))
    cc_custo = Column(String(50))
    nome_centro_custo = Column(String(130))
    
    # Contato
    endereco = Column(String(110))
    numero_endereco = Column(String(20))
    bairro = Column(String(80))
    cidade = Column(String(50))
    uf = Column(String(20))
    cep = Column(String(10))
    telefone_residencial = Column(String(20))
    telefone_celular = Column(String(20))
    telefone_comercial = Column(String(20))
    email = Column(String(400))
    ramal = Column(String(10))
    
    # Trabalho
    regime_revezamento = Column(Integer)
    regime_trabalho = Column(String(500))
    turno_trabalho = Column(Integer)
    rh_unidade = Column(String(80))
    rh_setor = Column(String(80))
    rh_cargo = Column(String(80))
    rh_centro_custo_unidade = Column(String(80))
    
    # PCD
    deficiente = Column(Boolean, default=False)
    deficiencia = Column(String(861))
    
    # Documentos
    pis = Column(String(20))
    ctps = Column(String(30))
    serie_ctps = Column(String(25))
    
    # SCD Tipo 2 - Controle de validade
    data_inicio_validade = Column(DateTime, default=datetime.utcnow, nullable=False)
    data_fim_validade = Column(DateTime)
    registro_ativo = Column(Boolean, default=True, nullable=False)
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    data_ultima_alteracao = Column(Date)
    
    # Relacionamentos
    empresa = relationship("DimEmpresa", back_populates="funcionarios")
    
    # Constraints
    __table_args__ = (
        Index('idx_funcionario_empresa', 'codigo_empresa'),
        Index('idx_funcionario_cpf', 'cpf'),
        Index('idx_funcionario_ativo', 'registro_ativo'),
        Index('idx_funcionario_natural', 'codigo_funcionario', 'codigo_empresa'),
    )


# =============================================================================
# TABELAS FATO
# =============================================================================

class FatoAbsenteismo(Base):
    """Fato Absenteísmo - Registros de afastamentos e atestados"""
    __tablename__ = 'fato_absenteismo'
    
    # Surrogate Key
    sk_absenteismo = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Keys para dimensões
    sk_funcionario = Column(Integer, ForeignKey('dim_funcionario.sk_funcionario'), nullable=False)
    sk_tempo_inicio = Column(Integer, ForeignKey('dim_tempo.sk_tempo'), nullable=False)
    sk_tempo_fim = Column(Integer, ForeignKey('dim_tempo.sk_tempo'))
    
    # Chave natural composta (para degreneração)
    codigo_empresa = Column(Integer, nullable=False)
    unidade = Column(String(130))
    setor = Column(String(130))
    data_nascimento = Column(Date, nullable=False)
    sexo = Column(Integer, nullable=False)
    matricula_funcionario = Column(String(30))
    
    # Atributos do evento
    tipo_atestado = Column(Integer, nullable=False)
    data_inicio_atestado = Column(Date, nullable=False)
    data_fim_atestado = Column(Date)
    hora_inicio_atestado = Column(String(5))
    hora_fim_atestado = Column(String(5))
    
    # Métricas
    dias_afastados = Column(Numeric(8, 2), default=0)
    horas_afastado = Column(String(5))
    
    # Informações médicas
    cid_principal = Column(String(10))
    descricao_cid = Column(String(264))
    grupo_patologico = Column(String(80))
    tipo_licenca = Column(String(100))
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    
    # Relacionamentos
    funcionario = relationship("DimFuncionario")
    tempo_inicio = relationship("DimTempo", foreign_keys=[sk_tempo_inicio])
    tempo_fim = relationship("DimTempo", foreign_keys=[sk_tempo_fim])
    
    # Constraints
    __table_args__ = (
        Index('idx_absenteismo_funcionario', 'sk_funcionario'),
        Index('idx_absenteismo_periodo', 'sk_tempo_inicio', 'sk_tempo_fim'),
        Index('idx_absenteismo_empresa', 'codigo_empresa'),
        Index('idx_absenteismo_cid', 'cid_principal'),
    )


class FatoConvocacao(Base):
    """Fato Convocação - Convocações para exames ocupacionais"""
    __tablename__ = 'fato_convocacao'
    
    # Surrogate Key
    sk_convocacao = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Keys
    sk_funcionario = Column(Integer, ForeignKey('dim_funcionario.sk_funcionario'), nullable=False)
    sk_exame = Column(Integer, ForeignKey('dim_exame.sk_exame'), nullable=False)
    sk_tempo_ultimo_pedido = Column(Integer, ForeignKey('dim_tempo.sk_tempo'))
    sk_tempo_resultado = Column(Integer, ForeignKey('dim_tempo.sk_tempo'))
    
    # Chave natural
    codigo_empresa = Column(Integer, nullable=False)
    codigo_funcionario = Column(Integer, nullable=False)
    codigo_exame = Column(Integer, nullable=False)
    
    # Atributos da convocação
    cpf_funcionario = Column(String(19))
    matricula = Column(String(30))
    data_admissao = Column(Date)
    nome_funcionario = Column(String(120))
    email_funcionario = Column(String(400))
    telefone_funcionario = Column(String(20))
    
    # Informações do exame
    nome_exame = Column(String(200))
    data_ultimo_pedido = Column(Date)
    data_resultado = Column(Date)
    periodicidade = Column(Integer)
    refazer = Column(Boolean, default=False)
    
    # Localização
    unidade = Column(String(130))
    cidade = Column(String(50))
    estado = Column(String(20))
    bairro = Column(String(80))
    endereco = Column(String(110))
    cep = Column(String(10))
    cnpj_unidade = Column(String(20))
    setor = Column(String(130))
    cargo = Column(String(130))
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    
    # Relacionamentos
    funcionario = relationship("DimFuncionario")
    exame = relationship("DimExame")
    tempo_ultimo_pedido = relationship("DimTempo", foreign_keys=[sk_tempo_ultimo_pedido])
    tempo_resultado = relationship("DimTempo", foreign_keys=[sk_tempo_resultado])
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('codigo_funcionario', 'codigo_exame', name='uk_funcionario_exame'),
        Index('idx_convocacao_funcionario', 'sk_funcionario'),
        Index('idx_convocacao_exame', 'sk_exame'),
        Index('idx_convocacao_empresa', 'codigo_empresa'),
    )


class FatoCat(Base):
    """Fato CAT - Comunicação de Acidentes de Trabalho"""
    __tablename__ = 'fato_cat'
    
    # Surrogate Key
    sk_cat = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Keys
    sk_funcionario = Column(Integer, ForeignKey('dim_funcionario.sk_funcionario'), nullable=False)
    sk_tempo_acidente = Column(Integer, ForeignKey('dim_tempo.sk_tempo'), nullable=False)
    sk_tempo_atendimento = Column(Integer, ForeignKey('dim_tempo.sk_tempo'))
    sk_tempo_registro = Column(Integer, ForeignKey('dim_tempo.sk_tempo'))
    
    # Chave natural
    codigo_empresa = Column(Integer, nullable=False)
    codigo_funcionario = Column(Integer, nullable=False)
    numero_cat = Column(String(20), nullable=False)
    
    # Dados do acidente
    data_acidente = Column(Date, nullable=False)
    hora_acidente = Column(Time)
    local_acidente = Column(String(200))
    especificacao_local_acidente = Column(Text)
    ocorrencia_acidente = Column(Text)
    parte_corpo_atingida = Column(String(200))
    tipo = Column(String(100))
    potencial_acidente = Column(Boolean, default=False)
    
    # Dados do atendimento
    data_atendimento = Column(Date)
    hora_atendimento = Column(Time)
    data_registro = Column(Date)
    data_ficha = Column(Date)
    
    # Consequências
    morte = Column(Boolean, default=False)
    aposentado = Column(Boolean, default=False)
    afastamento = Column(Boolean, default=False)
    afastamento_durante_tratamento = Column(Boolean, default=False)
    ultimo_dia_trabalho = Column(Date)
    
    # Métricas
    dias_perdidos = Column(Integer, default=0)
    dias_afastado = Column(Integer, default=0)
    custo = Column(Numeric(15, 2), default=0)
    
    # Informações organizacionais
    codigo_unidade = Column(String(20))
    codigo_setor = Column(String(20))
    area = Column(String(100))
    cnpj_local = Column(String(20))
    motivo = Column(Text)
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    
    # Relacionamentos
    funcionario = relationship("DimFuncionario")
    tempo_acidente = relationship("DimTempo", foreign_keys=[sk_tempo_acidente])
    tempo_atendimento = relationship("DimTempo", foreign_keys=[sk_tempo_atendimento])
    tempo_registro = relationship("DimTempo", foreign_keys=[sk_tempo_registro])
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('numero_cat', name='uk_numero_cat'),
        Index('idx_cat_funcionario', 'sk_funcionario'),
        Index('idx_cat_acidente', 'sk_tempo_acidente'),
        Index('idx_cat_empresa', 'codigo_empresa'),
    )


class FatoVencimento(Base):
    """Fato Vencimento - Vencimentos de documentos empresariais"""
    __tablename__ = 'fato_vencimento'
    
    # Surrogate Key
    sk_vencimento = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Keys
    sk_empresa = Column(Integer, ForeignKey('dim_empresa.sk_empresa'), nullable=False)
    sk_tempo_vencimento = Column(Integer, ForeignKey('dim_tempo.sk_tempo'), nullable=False)
    sk_tempo_ultimo_servico = Column(Integer, ForeignKey('dim_tempo.sk_tempo'))
    sk_tempo_previsao_servico = Column(Integer, ForeignKey('dim_tempo.sk_tempo'))
    
    # Chave natural
    codigo_empresa = Column(Integer, nullable=False)
    codigo_unidade = Column(String(20))
    codigo_produto = Column(String(20), nullable=False)
    
    # Dados da empresa/unidade
    nome_empresa = Column(String(200))
    nome_unidade = Column(String(130))
    status_unidade = Column(String(20))
    cnpj_unidade = Column(String(20))
    
    # Dados do documento/produto
    nome_produto = Column(String(200), nullable=False)
    data_vencimento = Column(Date, nullable=False)
    situacao = Column(String(50))
    grau_risco = Column(String(10))
    legenda = Column(String(100))
    
    # Serviços
    data_realizacao_ultimo_servico = Column(Date)
    data_previsao_ultimo_servico = Column(Date)
    observacao_ultimo_servico = Column(Text)
    
    # Localização
    endereco = Column(String(110))
    numero = Column(String(20))
    complemento = Column(String(50))
    bairro = Column(String(80))
    cidade = Column(String(50))
    cep = Column(String(10))
    estado = Column(String(20))
    
    # Classificação
    cnae = Column(String(20))
    cnae_2_0 = Column(String(20))
    cnae_7 = Column(String(20))
    
    # Métricas calculadas
    dias_para_vencimento = Column(Integer)
    vencido = Column(Boolean, default=False)
    critico = Column(Boolean, default=False)  # < 30 dias
    
    # Metadados
    data_carga = Column(DateTime, default=datetime.utcnow)
    
    # Relacionamentos
    empresa = relationship("DimEmpresa", back_populates="vencimentos")
    tempo_vencimento = relationship("DimTempo", foreign_keys=[sk_tempo_vencimento])
    tempo_ultimo_servico = relationship("DimTempo", foreign_keys=[sk_tempo_ultimo_servico])
    tempo_previsao_servico = relationship("DimTempo", foreign_keys=[sk_tempo_previsao_servico])
    
    # Constraints
    __table_args__ = (
        Index('idx_vencimento_empresa', 'sk_empresa'),
        Index('idx_vencimento_data', 'sk_tempo_vencimento'),
        Index('idx_vencimento_produto', 'codigo_produto'),
        Index('idx_vencimento_situacao', 'situacao'),
        Index('idx_vencimento_critico', 'critico', 'vencido'),
    )


# =============================================================================
# VIEWS PARA RELATÓRIOS AGREGADOS
# =============================================================================

class ViewAbsenteismoPorPeriodo(Base):
    """View agregada para análise de absenteísmo por período"""
    __tablename__ = 'view_absenteismo_periodo'
    
    ano = Column(Integer, primary_key=True)
    mes = Column(Integer, primary_key=True)
    codigo_empresa = Column(Integer, primary_key=True)
    total_afastamentos = Column(Integer)
    total_dias_afastados = Column(Numeric(10, 2))
    media_dias_por_afastamento = Column(Numeric(8, 2))
    funcionarios_afetados = Column(Integer)
    
    # Esta é uma view, não uma tabela física
    __table_args__ = {'info': {'is_view': True}}


class ViewVencimentosCriticos(Base):
    """View para documentos com vencimentos críticos"""
    __tablename__ = 'view_vencimentos_criticos'
    
    sk_vencimento = Column(Integer, primary_key=True)
    codigo_empresa = Column(Integer)
    nome_empresa = Column(String(200))
    nome_produto = Column(String(200))
    data_vencimento = Column(Date)
    dias_para_vencimento = Column(Integer)
    situacao = Column(String(50))
    grau_risco = Column(String(10))
    
    # Esta é uma view, não uma tabela física
    __table_args__ = {'info': {'is_view': True}}
