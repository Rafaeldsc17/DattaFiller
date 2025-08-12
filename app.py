import sys
import streamlit as st
import pandas as pd
import dask.dataframe as dd
import io
import os
import time
import secrets
import json
import smtplib
from email.mime.text import MIMEText
import preenchimento_planilhas
from CRED.cred import atualizar_planilha
from auth import carregar_usuarios, salvar_usuarios, hash_senha

st.set_page_config(
    page_title="DataFiller",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern styling
st.markdown(
    """ 
    <style>
    /* General layout */
    .css-18e3th9 {
        padding-top: 2rem;
        padding-right: 3rem;
        padding-left: 3rem;
        padding-bottom: 2rem;
        max-width: 900px;
        margin: auto;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Title styling */
    .title-style {
        font-weight: 700 !important;
        font-size: 2.8rem !important;
        color: #003366 !important;
        margin-bottom: 0.75rem !important;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Subtitle styling */
    .subtitle-style {
        font-weight: 600 !important;
        color: #007acc !important;
        margin-bottom: 1rem !important;
    }
    /* Sidebar styling */
    section[data-testid="stSidebar"] > div:first-child {
        background-color: #f0f4f8;
        padding: 1rem 1rem;
        border-radius: 8px;
        border: 1px solid #ddd;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Button styling */
    div.stButton > button {
        background-color: #007acc;
        color: white;
        border-radius: 6px;
        padding: 8px 16px;
        border: none;
        font-weight: 600;
        transition: background-color 0.3s ease;
    }
    div.stButton > button:hover {
        background-color: #005fa3;
    }
    /* Dataframe styling */
    .stDataFrame table {
        border-radius: 8px;
        border-collapse: separate;
        border-spacing: 0 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgb(0 0 0 / 0.14);
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .stDataFrame th, .stDataFrame td {
        padding: 12px 15px !important;
    }
    /* Mission lists */
    div[data-baseweb="list-item"] {
        font-size: 1.1rem;
        margin-bottom: 6px;
        color: #333333;
    }
    /* Input fields */
    input, select {
        border-radius: 6px !important;
        border: 1px solid #ccc !important;
        padding: 8px !important;
        font-size: 1rem !important;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif !important;
    }
    /* Section dividers */
    hr {
        border: 0;
        height: 1px;
        background: #cccccc;
        margin: 2rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True
)

MISSOES_PATH = "missoes.json"
if not os.path.exists(MISSOES_PATH):
    with open(MISSOES_PATH, "w") as f:
        json.dump({"usuarios": {}, "grupos": {}}, f)

def carregar_missoes():
    with open(MISSOES_PATH, "r") as f:
        return json.load(f)

def salvar_missoes(missoes):
    with open(MISSOES_PATH, "w") as f:
        json.dump(missoes, f, indent=2)

def enviar_email(destinatario, token):
    remetente = "seu_email@gmail.com"
    senha_email = "abcd efgh ijkl mnop"  # ← a senha de app (sem espaços)
    smtp_servidor = "smtp.gmail.com"
    smtp_porta = 587

    assunto = "Redefinição de Senha DataFiller"
    corpo = f"""
Olá,

Você solicitou a redefinição de senha para sua conta DataFiller.

Use o token abaixo para redefinir sua senha. O token é válido por 15 minutos:

{token}

Caso você não tenha solicitado essa redefinição, ignore este e-mail.

Atenciosamente,
Equipe DataFiller
"""

    msg = MIMEText(corpo)
    msg['Subject'] = assunto
    msg['From'] = remetente
    msg['To'] = destinatario

    try:
        with smtplib.SMTP(smtp_servidor, smtp_porta) as server:
            server.starttls()
            server.login(remetente, senha_email)
            server.sendmail(remetente, destinatario, msg.as_string())
        st.success("Token enviado para o seu e-mail.")
    except Exception as e:
        st.error(f"Erro ao enviar e-mail: {e}")
        st.info(f"(Simulação) Token para redefinição: {token}")

if "logado" not in st.session_state:
    st.session_state.logado = False

usuarios = carregar_usuarios()

# --- LOGIN ---
if not st.session_state.logado:
    st.markdown('<h1 class="title-style">DataFiller Login</h1>', unsafe_allow_html=True)
    modo = st.radio("Selecione uma opção", ["🔐 Entrar", "🆕 Criar conta", "🔑 Redefinir senha"])

    nome = st.text_input("Nome").strip().lower()
    sobrenome = st.text_input("Sobrenome").strip().lower()
    chave_usuario = f"{nome}_{sobrenome}"

    if modo == "🔐 Entrar":
        senha = st.text_input("Senha", type="password")
        if chave_usuario in usuarios:
            usuario = usuarios[chave_usuario]
            if usuario.get("tentativas", 0) >= 5:
                st.warning("Muitas tentativas falhas. Clique em 'Redefinir senha'.")
                st.stop()

        if st.button("Entrar"):
            senha_hash = hash_senha(senha)
            if chave_usuario in usuarios and usuarios[chave_usuario]["senha"] == senha_hash:
                usuarios[chave_usuario]["tentativas"] = 0
                salvar_usuarios(usuarios)
                st.session_state.logado = True
                st.session_state.nome = nome.capitalize()
                st.session_state.sobrenome = sobrenome.capitalize()
                st.session_state.usuario_chave = chave_usuario
                st.session_state.nivel = usuarios[chave_usuario].get("nivel", "usuario")
                st.session_state.grupo = usuarios[chave_usuario].get("grupo", "")
                st.session_state.acesso_planilhas = usuarios[chave_usuario].get("acesso_planilhas", False)
                st.session_state.acesso_credfranco = usuarios[chave_usuario].get("acesso_credfranco", False)
                st.session_state.argos_coins = usuarios[chave_usuario].get("argos_coins", 0)
                st.rerun()
            else:
                if chave_usuario in usuarios:
                    usuarios[chave_usuario]["tentativas"] = usuarios[chave_usuario].get("tentativas", 0) + 1
                    salvar_usuarios(usuarios)
                st.error("Nome, sobrenome ou senha incorretos.")
        st.stop()

    elif modo == "🆕 Criar conta":
        senha = st.text_input("Senha", type="password")
        email = st.text_input("Email").strip().lower()

        grupos_existentes = sorted(set(v.get("grupo", "") for v in usuarios.values() if v.get("grupo")))
        grupo_escolhido = st.selectbox("Selecione um grupo existente:", grupos_existentes) if grupos_existentes else ""

        if st.button("Criar conta"):
            if chave_usuario in usuarios:
                st.warning("Usuário já existe. Tente outro nome.")
            elif not nome or not sobrenome or not senha or not email:
                st.warning("Preencha todos os campos.")
            elif not grupo_escolhido:
                st.warning("Aguarde um administrador criar um grupo.")
            elif "@" not in email or "." not in email:
                st.warning("Insira um e-mail válido.")
            else:
                usuarios[chave_usuario] = {
                    "senha": hash_senha(senha),
                    "email": email,
                    "tentativas": 0,
                    "token": None,
                    "expira_em": None,
                    "nivel": "usuario",
                    "grupo": grupo_escolhido,
                    "acesso_planilhas": False,
                    "acesso_credfranco": False,
                    "argos_coins": 0
                }
                salvar_usuarios(usuarios)
                st.success("Conta criada com sucesso! Agora você pode fazer login.")
        st.stop()

    elif modo == "🔑 Redefinir senha":
        if chave_usuario not in usuarios:
            st.warning("Usuário não encontrado.")
            st.stop()

        usuario = usuarios[chave_usuario]
        if usuario["token"] is None:
            if st.button("Enviar link de redefinição para o e-mail"):
                token = secrets.token_urlsafe(16)
                usuario["token"] = token
                usuario["expira_em"] = time.time() + 900
                salvar_usuarios(usuarios)
                enviar_email(usuario['email'], token)  # Chama a função para enviar o e-mail
            st.stop()
        else:
            token_input = st.text_input("Digite o token recebido no e-mail")
            nova_senha = st.text_input("Nova senha", type="password")
            if st.button("Redefinir senha"):
                if time.time() > usuario["expira_em"]:
                    st.error("Token expirado. Solicite um novo.")
                    usuario["token"] = None
                    usuario["expira_em"] = None
                    salvar_usuarios(usuarios)
                elif token_input != usuario["token"]:
                    st.error("Token incorreto.")
                else:
                    usuario["senha"] = hash_senha(nova_senha)
                    usuario["tentativas"] = 0
                    usuario["token"] = None
                    usuario["expira_em"] = None
                    salvar_usuarios(usuarios)
                    st.success("Senha redefinida com sucesso! Agora você pode fazer login.")
            st.stop()

# --- FUNÇÃO PARA MOSTRAR MISSÕES NA TELA INICIAL ---
def mostrar_missoes_iniciais():
    st.markdown('<h1 class="title-style">📌 Suas Missões</h1>', unsafe_allow_html=True)

    missoes = carregar_missoes()
    usuario_chave = st.session_state.usuario_chave
    nivel = st.session_state.nivel

    missoes_usuario = missoes["usuarios"].get(usuario_chave, {"ganhos": [], "perdas": []})
    ganhos_usuario = missoes_usuario.get("ganhos", [])
    perdas_usuario = missoes_usuario.get("perdas", [])

    def mostrar_lista_missoes(missoes_lista, titulo):
        if missoes_lista:
            st.markdown(f'### {titulo}')
            for m in missoes_lista:
                st.markdown(f"- {m['descricao']} ({'+' if titulo == 'Ganhos' else '-'}{m['valor']} ArgosCoins)")
        else:
            st.markdown(f"*Sem missões de {titulo.lower()}.*")

    mostrar_lista_missoes(ganhos_usuario, "Ganhos")
    mostrar_lista_missoes(perdas_usuario, "Perdas")

# --- PAINEL DE MISSÕES (admin) COM REGISTRO DE COMPLETAÇÃO ---
def painel_missoes():
    st.markdown('<h1 class="title-style">📌 Painel de Missões</h1>', unsafe_allow_html=True)
    missoes = carregar_missoes()

    tipo = st.radio("Tipo de missão", ["Usuário", "Grupo"], key="tipo_missao")

    if tipo == "Usuário":
        alvo = st.selectbox("Selecionar usuário:", options=list(usuarios.keys()))
        missao_atual = missoes["usuarios"].get(alvo, {"ganhos": [], "perdas": [], "concluidas": []})
    else:
        grupos_disponiveis = sorted(set(v.get("grupo", "") for v in usuarios.values() if v.get("grupo")))
        alvo = st.selectbox("Selecionar grupo:", options=grupos_disponiveis)
        missao_atual = missoes["grupos"].get(alvo, {"ganhos": [], "perdas": [], "concluidas": []})

    if "concluidas" not in missao_atual:
        missao_atual["concluidas"] = []

    st.markdown("#### Ganhos")
    for i, m in enumerate(missao_atual["ganhos"]):
        col1, col2, col3, col4 = st.columns([0.6, 0.15, 0.15, 0.15])
        with col1:
            st.markdown(f"**{m['descricao']}** (+{m['valor']} ArgosCoins)")
        with col2:
            if st.button("Remover", key=f"rm_ganho_{tipo}_{alvo}_{i}"):
                missao_atual["ganhos"].pop(i)
                salvar_missoes(missoes)
                st.rerun()
        with col3:
            if tipo == "Usuário" and st.button(f"Concluir {i}", key=f"concluir_ganho_{alvo}_{i}"):
                if alvo in usuarios:
                    usuarios[alvo]["argos_coins"] = usuarios[alvo].get("argos_coins", 0) + m["valor"]
                    salvar_usuarios(usuarios)
                    st.success(f"Missão '{m['descricao']}' concluída! {m['valor']} ArgosCoins adicionados a {alvo}.")
                    missao_atual["concluidas"].append({"descricao": m["descricao"], "valor": m["valor"], "tipo": "ganho", "quando": time.strftime("%Y-%m-%d %H:%M:%S")})
                    salvar_missoes(missoes)
                    st.rerun()
        with col4:
            if tipo == "Grupo" and st.button(f"Concluir para grupo {i}", key=f"concluir_ganho_grupo_{alvo}_{i}"):
                grupo_usuarios = {k: v for k, v in usuarios.items() if v.get("grupo") == alvo}
                for u in grupo_usuarios:
                    usuarios[u]["argos_coins"] = usuarios[u].get("argos_coins", 0) + m["valor"]
                salvar_usuarios(usuarios)
                st.success(f"Missão '{m['descricao']}' concluída para o grupo {alvo}! {m['valor']} ArgosCoins adicionados a todos.")
                missao_atual["concluidas"].append({"descricao": m["descricao"], "valor": m["valor"], "tipo": "ganho", "quando": time.strftime("%Y-%m-%d %H:%M:%S")})
                salvar_missoes(missoes)
                st.rerun()

    st.markdown("#### Perdas")
    for i, m in enumerate(missao_atual["perdas"]):
        col1, col2, col3, col4 = st.columns([0.6, 0.15, 0.15, 0.15])
        with col1:
            st.markdown(f"**{m['descricao']}** (-{m['valor']} ArgosCoins)")
        with col2:
            if st.button("Remover", key=f"rm_perda_{tipo}_{alvo}_{i}"):
                missao_atual["perdas"].pop(i)
                salvar_missoes(missoes)
                st.rerun()
        with col3:
            if tipo == "Usuário" and st.button(f"Concluir {i}", key=f"concluir_perda_{alvo}_{i}"):
                if alvo in usuarios:
                    novo_saldo = usuarios[alvo]["argos_coins"] - m["valor"]
                    usuarios[alvo]["argos_coins"] = max(novo_saldo, 0)
                    salvar_usuarios(usuarios)
                    st.success(f"Missão '{m['descricao']}' concluída! {m['valor']} ArgosCoins descontados de {alvo}.")
                    missao_atual["concluidas"].append({"descricao": m["descricao"], "valor": m["valor"], "tipo": "perda", "quando": time.strftime("%Y-%m-%d %H:%M:%S")})
                    salvar_missoes(missoes)
                    st.rerun()
        with col4:
            if tipo == "Grupo" and st.button(f"Concluir para grupo {i}", key=f"concluir_perda_grupo_{alvo}_{i}"):
                grupo_usuarios = {k: v for k, v in usuarios.items() if v.get("grupo") == alvo}
                for u in grupo_usuarios:
                    novo_saldo = usuarios[u].get("argos_coins",0) - m["valor"]
                    usuarios[u]["argos_coins"] = max(novo_saldo, 0)
                salvar_usuarios(usuarios)
                st.success(f"Missão '{m['descricao']}' concluída para o grupo {alvo}! {m['valor']} ArgosCoins descontados de todos.")
                missao_atual["concluidas"].append({"descricao": m["descricao"], "valor": m["valor"], "tipo": "perda", "quando": time.strftime("%Y-%m-%d %H:%M:%S")})
                salvar_missoes(missoes)
                st.rerun()

    nova_ganho = st.text_input("Nova missão de ganho")
    valor_ganho = st.number_input("Valor ArgosCoins (ganha)", min_value=1, step=1, key="ganho")
    if st.button("Adicionar ganho"):
        if nova_ganho.strip() != "":
            missao_atual["ganhos"].append({"descricao": nova_ganho, "valor": valor_ganho})
            salvar_missoes(missoes)
            st.success("Ganho adicionado!")
            st.rerun()

    nova_perda = st.text_input("Nova missão de perda")
    valor_perda = st.number_input("Valor ArgosCoins (perde)", min_value=1, step=1, key="perda")
    if st.button("Adicionar perda"):
        if nova_perda.strip() != "":
            missao_atual["perdas"].append({"descricao": nova_perda, "valor": valor_perda})
            salvar_missoes(missoes)
            st.success("Perda adicionada!")
            st.rerun()

    if tipo == "Usuário":
        missoes["usuarios"][alvo] = missao_atual
    else:
        missoes["grupos"][alvo] = missao_atual

    salvar_missoes(missoes)

    st.markdown("#### Missões Concluídas")
    if missao_atual["concluidas"]:
        for concl in reversed(missao_atual["concluidas"][-10:]):
            sinal = "+" if concl["tipo"] == "ganho" else "-"
            st.markdown(f"{concl['quando']}: {concl['descricao']} ({sinal}{concl['valor']} ArgosCoins)")
    else:
        st.markdown("Nenhuma missão concluída ainda.")

# --- PAINEL SUPERVISOR: MOSTRAR USUÁRIOS DO MESMO GRUPO E MISSÕES DO GRUPO ---
def painel_supervisor():
    st.markdown('<h1 class="title-style">Painel do Supervisor</h1>', unsafe_allow_html=True)

    grupo = st.session_state.grupo
    st.markdown(f'### Usuários do seu grupo: {grupo}')

    usuarios_no_grupo = {k: v for k, v in usuarios.items() if v.get("grupo") == grupo}
    if usuarios_no_grupo:
        df = pd.DataFrame([
            {
                "Usuário": k,
                "ArgosCoins": v.get("argos_coins", 0),
            }
            for k, v in usuarios_no_grupo.items()
        ])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum usuário encontrado em seu grupo.")

    st.markdown("---")
    st.markdown("### Missões do Grupo")

    missoes = carregar_missoes()
    missoes_grupo = missoes["grupos"].get(grupo, {"ganhos": [], "perdas": []})

    if missoes_grupo.get("ganhos"):
        st.markdown("**Ganhos:**")
        for m in missoes_grupo["ganhos"]:
            st.markdown(f"- {m['descricao']} (+{m['valor']} ArgosCoins)")
    else:
        st.markdown("*Sem missões de ganhos.*")

    if missoes_grupo.get("perdas"):
        st.markdown("**Perdas:**")
        for m in missoes_grupo["perdas"]:
            st.markdown(f"- {m['descricao']} (-{m['valor']} ArgosCoins)")
    else:
        st.markdown("*Sem missões de perdas.*")

# --- MENU PÓS LOGIN ---
if st.session_state.logado:
    if st.session_state.nivel == "supervisor":
        mostrar_missoes_iniciais()
        st.markdown("---")
        painel_supervisor()
    else:
        mostrar_missoes_iniciais()

st.sidebar.markdown(f"👋 Olá, **{st.session_state.nome} {st.session_state.sobrenome}**")
st.sidebar.write(f"🔐 Nível de acesso: {st.session_state.nivel}")
st.sidebar.write(f"ArgosCoins: 💰 {st.session_state.get('argos_coins', 0)} ({st.session_state.get('argos_coins', 0) * 0.10:.2f} R$)")
if st.sidebar.button("🔓 Logout"):
    st.session_state.clear()
    st.rerun()

menu = st.sidebar.radio(
    "Menu",
    ["🏠 Início"] +
    (["📊 Preenchimento de dados"] if st.session_state.acesso_planilhas else []) +
    (["💼 CredFranco"] if st.session_state.acesso_credfranco else []) +
    (["📌 Missões"] if st.session_state.nivel == "admin" else []) +
    (["🛡 Admin"] if st.session_state.nivel == "admin" else [])
)

if menu == "📌 Missões":
    if st.session_state.nivel == "admin":
        painel_missoes()
    else:
        st.warning("Você não tem acesso a este painel.")

if menu == "🛡 Admin":
    st.markdown('<h1 class="title-style">Painel do Administrador</h1>', unsafe_allow_html=True)
    usuarios = carregar_usuarios()
    grupos = sorted(set(v.get("grupo", "") for v in usuarios.values() if v.get("grupo")))

    for grupo in grupos:
        st.markdown(f'### Grupo: {grupo}')
        usuarios_grupo = {k: v for k, v in usuarios.items() if v.get("grupo") == grupo}
        if usuarios_grupo:
            df = pd.DataFrame([
                {
                    "Usuário": k,
                    "Nível": v.get("nivel", ""),
                    "Email": v.get("email", ""),
                    "Planilhas": v.get("acesso_planilhas", False),
                    "CredFranco": v.get("acesso_credfranco", False),
                    "ArgosCoins": v.get("argos_coins", 0)
                }
                for k, v in usuarios_grupo.items()
            ])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Nenhum usuário neste grupo.")

    editar = st.selectbox("✏️ Editar usuário:", options=list(usuarios.keys()))
    
    grupos_existentes = sorted(set(v.get("grupo", "") for v in usuarios.values() if v.get("grupo")))
    grupo_atual = usuarios[editar].get("grupo", "")

    grupo_selecionado = st.selectbox(
        "👥 Selecionar grupo existente:",
        options=[""] + grupos_existentes,
        index=(grupos_existentes.index(grupo_atual) + 1) if grupo_atual in grupos_existentes else 0
    )

    novo_grupo_texto = st.text_input("Ou digite o nome do novo grupo:", value="")

    novo_grupo = novo_grupo_texto.strip() if novo_grupo_texto.strip() != "" else grupo_selecionado

    novo_nivel = st.selectbox("🔧 Novo nível:", ["usuario", "supervisor", "admin"],
                              index=["usuario", "supervisor", "admin"].index(usuarios[editar].get("nivel", "usuario")))
    acesso_planilhas = st.checkbox("📊 Acesso Preenchimento de dados", value=usuarios[editar].get("acesso_planilhas", False))
    acesso_credfranco = st.checkbox("💼 Acesso CredFranco", value=usuarios[editar].get("acesso_credfranco", False))
    argos_coins = st.number_input("💰 ArgosCoins", min_value=0, value=int(usuarios[editar].get("argos_coins", 0)), step=1)

    if st.button("💾 Salvar alterações"):
        usuarios[editar]["nivel"] = novo_nivel
        usuarios[editar]["grupo"] = novo_grupo
        usuarios[editar]["acesso_planilhas"] = acesso_planilhas
        usuarios[editar]["acesso_credfranco"] = acesso_credfranco
        usuarios[editar]["argos_coins"] = argos_coins
        salvar_usuarios(usuarios)
        st.success("✅ Usuário atualizado com sucesso!")
        st.rerun()

if menu == "📊 Preenchimento de dados":
    preenchimento_planilhas.executar()

if menu == "💼 CredFranco":
    preenchimento_planilhas.executar_credfranco()