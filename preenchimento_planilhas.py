import pandas as pd
import dask.dataframe as dd
import io
import os
import streamlit as st
from CRED.cred import atualizar_planilha

def formatar_valor(v):
    if pd.isna(v):
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)

def read_file(file, MODELS_DIR):
    if file is not None:
        if isinstance(file, str):
            path = os.path.join(MODELS_DIR, file)
            if file.endswith((".xls", ".xlsx")):
                return pd.read_excel(path)
            else:
                return pd.read_csv(path, delimiter=";")
        elif file.name.endswith((".xls", ".xlsx")):
            return pd.read_excel(file)
        elif file.name.endswith((".csv", ".txt")):
            try:
                file_bytes = io.BytesIO(file.read())
                return dd.read_csv(file_bytes, delimiter=";").compute()
            except Exception:
                file.seek(0)
                return pd.read_csv(file, delimiter=";")
    return None

def executar():
    st.title("\U0001F4CA DataFiller ")

    MODELS_DIR = "modelos_salvos"
    os.makedirs(MODELS_DIR, exist_ok=True)
    modelos_disponiveis = sorted([m for m in os.listdir(MODELS_DIR) if m.endswith((".xlsx", ".xls", ".csv", ".txt"))])

    usar_modelo_salvo = st.radio("\U0001F4C2 Como deseja usar o modelo?", ["\U0001F4C4 Enviar novo", "\U0001F4C1 Usar modelo salvo"])

    uploaded_modelo = None
    selected_modelo_nome = None
    salvar_novo_modelo = False
    nome_para_salvar = None

    if usar_modelo_salvo == "\U0001F4C1 Usar modelo salvo":
        if modelos_disponiveis:
            selected_modelo_nome = st.selectbox("\U0001F4C1 Selecione um modelo salvo:", modelos_disponiveis)
        else:
            st.warning("\u26A0\ufe0f Nenhum modelo salvo encontrado. Envie um novo abaixo.")
            usar_modelo_salvo = "\U0001F4C4 Enviar novo"
    elif usar_modelo_salvo == "\U0001F4C4 Enviar novo":
        uploaded_modelo = st.file_uploader("\U0001F4C2 Envie a planilha MODELO", type=["xlsx", "xls", "csv", "txt"])
        salvar_novo_modelo = st.checkbox("\U0001F4BE Salvar este modelo para uso futuro")
        if salvar_novo_modelo:
            nome_para_salvar = st.text_input("\U0001F4DD Nome para salvar", value="modelo_novo").replace(" ", "_")

    uploaded_base = st.file_uploader("\U0001F4C2 Envie a planilha BASE", type=["xlsx", "xls", "csv", "txt"])
    uploaded_aux = st.file_uploader("\U0001F4C2 (Opcional) Envie a planilha AUXILIAR", type=["xlsx", "xls", "csv", "txt"])

    df_base = read_file(uploaded_base, MODELS_DIR)
    df_aux = read_file(uploaded_aux, MODELS_DIR) if uploaded_aux else None
    df_modelo = read_file(selected_modelo_nome if usar_modelo_salvo == "\U0001F4C1 Usar modelo salvo" else uploaded_modelo, MODELS_DIR)

    if uploaded_modelo and salvar_novo_modelo and nome_para_salvar:
        caminho = os.path.join(MODELS_DIR, f"{nome_para_salvar}.xlsx")
        try:
            df_modelo.to_excel(caminho, index=False)
            st.success(f"✅ Modelo salvo como '{nome_para_salvar}.xlsx'")
        except Exception as e:
            st.error(f"❌ Erro ao salvar o modelo: {e}")

    if df_base is not None and df_modelo is not None:
        st.subheader("\U0001F4CA Visualização das planilhas")
        st.write("\U0001F539 **BASE**")
        st.dataframe(df_base.head())
        st.write("\U0001F539 **MODELO**")
        st.dataframe(df_modelo.head())
        if df_aux is not None:
            st.write("\U0001F539 **AUXILIAR**")
            st.dataframe(df_aux.head())

        st.divider()
        st.subheader("🔁 Mapeamento para preenchimento")

        preenchimento_modelo = {}
        separador_padrao = st.text_input("✂️ Separador para concatenação:", value=" ")

        for col_modelo in df_modelo.columns:
            st.markdown(f"### Coluna: `{col_modelo}`")
            colunas_opcoes = [""] + df_base.columns.tolist()
            selecionadas = st.multiselect(f"Colunas da BASE para preencher `{col_modelo}`:", options=colunas_opcoes, default=[], key=f"map_{col_modelo}")
            valor_fixo = st.text_input(f"Valor fixo para `{col_modelo}`:", key=f"fixo_{col_modelo}")
            preenchimento_modelo[col_modelo] = {"selecionadas": selecionadas, "fixo": valor_fixo}

        aux_mapping = {}
        col_modelo_ref = col_aux_ref = None
        pegar_primeiro = st.radio("Se houver múltiplos valores:", ("Pegar o primeiro", "Pegar todos"))

        if df_aux is not None:
            st.divider()
            st.subheader("\U0001F50D Cruzamento com AUXILIAR")

            col_modelo_ref = st.selectbox("Coluna MODELO de referência:", df_modelo.columns)
            col_aux_ref = st.selectbox("Coluna AUXILIAR correspondente:", df_aux.columns)

            for col_aux in df_aux.columns:
                destino = st.selectbox(f"Coluna AUXILIAR `{col_aux}` → coluna da MODELO:", [""] + df_modelo.columns.tolist(), key="aux_" + col_aux)
                if destino:
                    aux_mapping[col_aux] = destino

        if st.button("\U0001F680 Executar preenchimento"):
            df_result = df_modelo.copy()

            try:
                for col_modelo, dados in preenchimento_modelo.items():
                    fixo = dados["fixo"]
                    selecionadas = [col for col in dados["selecionadas"] if col]
                    if fixo:
                        df_result[col_modelo] = fixo
                    elif not selecionadas:
                        df_result[col_modelo] = ""
                    elif len(selecionadas) == 1:
                        df_result[col_modelo] = df_base[selecionadas[0]].apply(formatar_valor)
                    else:
                        df_result[col_modelo] = df_base[selecionadas].applymap(formatar_valor).apply(lambda row: separador_padrao.join(row.values), axis=1)
            except Exception as e:
                st.error(f"Erro na base: {e}")

            if df_aux is not None and col_modelo_ref and col_aux_ref:
                try:
                    df_result[col_modelo_ref] = df_result[col_modelo_ref].astype(str).fillna("").str.upper().str.strip()
                    df_aux[col_aux_ref] = df_aux[col_aux_ref].astype(str).fillna("").str.upper().str.strip()

                    if pegar_primeiro == "Pegar o primeiro":
                        df_aux_first = df_aux.drop_duplicates(subset=col_aux_ref, keep="first")
                        df_merge = df_result.merge(df_aux_first, how="left", left_on=col_modelo_ref, right_on=col_aux_ref)
                    else:
                        df_merge = df_result.merge(df_aux, how="left", left_on=col_modelo_ref, right_on=col_aux_ref)

                    for col_aux, destino in aux_mapping.items():
                        if col_aux in df_merge.columns:
                            df_merge[destino] = df_merge[col_aux].apply(formatar_valor)

                    df_result = df_merge[df_modelo.columns]
                except Exception as e:
                    st.error(f"Erro no cruzamento: {e}")

            st.success("✅ Preenchimento finalizado!")
            st.dataframe(df_result.head(5))

            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df_result.to_excel(writer, index=False)
            st.download_button("📥 Baixar MODELO preenchido", data=buffer.getvalue(), file_name="modelo_final.xlsx")

def executar_credfranco():
    st.title("Atualização CredFranco")

    uploaded_base = st.file_uploader("📂 Envie a tabela base CSV ou Excel", type=['csv', 'xls', 'xlsx'])
    uploaded_modelo = st.file_uploader("📂 Envie a tabela modelo Excel", type=['xlsx'])

    banco = st.text_input("🏦 Nome do banco")
    convenio = st.text_input("📄 Nome do convênio")

    if uploaded_base and uploaded_modelo and banco and convenio:
        base_path = "base_temp.csv"
        modelo_path = "modelo_temp.xlsx"
        output_path = "saida_preenchida.xlsx"

        if uploaded_base.name.endswith(".csv"):
            with open(base_path, "wb") as f:
                f.write(uploaded_base.getbuffer())
        else:
            df_base = pd.read_excel(uploaded_base)
            df_base.to_csv(base_path, index=False)

        with open(modelo_path, "wb") as f:
            f.write(uploaded_modelo.getbuffer())

        try:
            atualizar_planilha(base_path, modelo_path, banco, convenio, output_path)
            st.success("✅ Processamento concluído com sucesso!")

            with open(output_path, "rb") as f:
                st.download_button("📥 Baixar arquivo preenchido", f, file_name="tabela_modelo_preenchida.xlsx")

        except Exception as e:
            st.error(f"❌ Erro no processamento: {e}")
