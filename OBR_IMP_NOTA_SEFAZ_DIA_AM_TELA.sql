CREATE OR REPLACE PROCEDURE OBR_IMP_NOTA_SEFAZ_DIA_AM_TELA(p_EstCodigo  VARCHAR2
                                                          ,p_linha_xml  VARCHAR2
                                                          ,p_num_linha  VARCHAR2
                                                          ,p_terminou   BOOLEAN DEFAULT FALSE
                                                          ,p_excluir    VARCHAR2 DEFAULT 'N'
                                                          ,p_DtExclusao VARCHAR2 DEFAULT NULL
                                                          ,p_synchro4me BOOLEAN DEFAULT FALSE
                                                          ,p_clob       CLOB DEFAULT NULL) IS

    -- Person  Ocorrencia Date         Comments
    -- ----------------------------------------------------------------------------------------------------------------------------------------------------------
    -- KAQ     AD.3174111 08/12/2020 - Criacao do Objeto. Essa procedure tem a finalidade de Importar o xml de Notas da Sefaz por tela
    -- TBT/CBF AD.3221423 07/03/2022 - Tratamento para remover quot e ajuste na selecao da vigencia.
    -- IFA     AD 3238780 10/08/2022 - Alteracao para importar o arquivo CLOB de uma so vez quando e synchro4me.
    -- FCP     AD.3295866 02/01/2024 - Alteracao no calculo do multiplicador
    -- FCP     AD.3305766 15/03/2024 - Alteracao no calculo do multiplicador
    -- FCP     AD.3322746 08/08/2024 - Considerar parametros 13 e 14 e adequacao do campo cod_tributacao, para que fique de acordo com a obrigacao
    -- FCP     AD.3325264 28/08/2024 - Ajuste evitar erro de conversao para numerico
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Variaveis para o tutl_fileratamento do arquivo.
    Text_xml    CLOB;
    tXml        xmltype;
    v_Xml       xmltype;
    ordem       NUMBER DEFAULT 0;
    ordem_item  NUMBER DEFAULT 0;
    Imp_Sucesso NUMBER DEFAULT 0;
    Importado   BOOLEAN DEFAULT FALSE;
    -- Variaveis de Conteudo das Tags
    ie_contribuinte      VARCHAR2(20);
    anoApresentacao      VARCHAR2(4);
    mesApresentacao      VARCHAR2(2);
    chaveNFe             VARCHAR2(50);
    numItens             VARCHAR2(3);
    numItemNFe           VARCHAR2(3);
    codProdutoNFe        VARCHAR2(50);
    descProduto          VARCHAR2(50);
    codEAN               VARCHAR2(20);
    codNCM               VARCHAR2(20);
    valorItemNFe         VARCHAR2(20);
    iOrdemIDF            NUMBER;
    vCodInternoItem      VARCHAR2(60);
    vBaseCalculo         NUMBER(19, 2);
    v_qt_item            NUMBER;
    v_codTipoTributacao    VARCHAR2(4);
    v_Uf_Codigo_Emitente cor_dof.uf_codigo_emitente%TYPE;
    v_Lista_tipo         syn_lista_valores.lista_tipo%TYPE;
    v_mun_codigo         cor_localidade_vigencia.mun_codigo%TYPE;
    v_vl_Multiplicador   VARCHAR2(10);
    v_valor_imposto      obr_imp_nota_sefaz_dia_am_item.vl_imposto%TYPE;
    vIndice              OBR_IMP_NOTA_SEFAZ_DIA_AM_ITEM.INDICE%TYPE;
    cst                  varchar(1000);

    TYPE tdoc_fiscais IS RECORD(
         vCodInternoItem      VARCHAR2(60)
        ,vBaseCalculo         NUMBER(19, 2)
        ,vDtEmissao           DATE
        ,vCFOPCodigo          VARCHAR2(10)
        ,vEmitente_pfj_codigo VARCHAR2(20)
        ,vIDFNum              NUMBER
        ,vReconhece           VARCHAR2(01)
        ,vOmCodigo            VARCHAR2(01) --FCP AD.3305766 - 14/03/2024
        ,vVlDifa              NUMBER(19,6) --FCP AD.3305766 - 14/03/2024
        ,vIndEntradaSaida     VARCHAR2(01) --FCP AD.3322746 - 08/08/2024
        ,vStcCodigo           VARCHAR2(02) --FCP AD.3322746 - 08/08/2024  
        ,vNOPCodigo           VARCHAR2(20) --FCP AD.3322746 - 08/08/2024 
        ,vTipo                VARCHAR2(01) --FCP AD.3322746 - 08/08/2024                
        ,vTipoComplemento     VARCHAR2(02) --FCP AD.3322746 - 08/08/2024 
        ,vVlStf               NUMBER(19, 2)   --FCP AD.3322746 - 08/08/2024 
        ,vVlStt               NUMBER(19, 2)); --FCP AD.3322746 - 08/08/2024 
        
    TYPE TableDocFiscais IS TABLE OF tdoc_fiscais INDEX BY BINARY_INTEGER;

    vdoc_fiscais        TableDocFiscais;
    vdoc_fiscais_Limpar TableDocFiscais;

    CURSOR cDoc_Fiscais(chaveNFe VARCHAR2) IS
        SELECT idf.vl_contabil
              ,dof.dh_emissao
              ,idf.merc_codigo
              ,idf.cfop_codigo
              ,dof.emitente_pfj_codigo
              ,idf.om_codigo     --FCP - AD.3305766 - 14/03/2023 
              ,idf.vl_difa       --FCP - AD.3305766 - 14/03/2023 
              ,dof.ind_entrada_saida   --FCP AD.3322746 - 08/08/2024
              ,dof.tipo                --FCP AD.3322746 - 08/08/2024
              ,idf.tipo_complemento    --FCP AD.3322746 - 08/08/2024
              ,idf.vl_stf              --FCP AD.3322746 - 08/08/2024
              ,idf.vl_stt              --FCP AD.3322746 - 08/08/2024
              ,idf.stc_codigo          --FCP AD.3322746 - 08/08/2024
              ,idf.nop_codigo          --FCP AD.3322746 - 08/08/2024
              ,rownum
          FROM cor_dof dof
              ,cor_idf idf
         WHERE dof.nfe_localizador = chaveNFe
           AND dof.dof_sequence = idf.dof_sequence
           AND dof.codigo_do_site = idf.codigo_do_site
           AND dof.informante_est_codigo = p_EstCodigo
         ORDER BY idf_num;

    rDoc_Fiscais cDoc_Fiscais%ROWTYPE;

    ------------------------------------------------------------------------------
    FUNCTION get_value_tag(aXml xmlType
                          ,pTag VARCHAR2) RETURN VARCHAR2 IS
        v_tag VARCHAR2(3000);
        x1    XMLType;
        v1    VARCHAR2(100);
        
        
    BEGIN
    
        v_tag := pTag || '/text()';
        
        x1    := aXml.extract(v_tag, 'xmlns="http://www.sefaz.am.gov.br/autodesembaraco"');
        
        IF x1 IS NOT NULL THEN
            v1 := x1.getstringval();
            v1 := REPLACE(v1, '&quot;', '"'); -- TBT/CBF AD.3221423 07/03/2022
            v1 := REPLACE(v1, '&amp;', '"');  --IFA AD 3238780 10/08/2022
        ELSE
            v1 := NULL;
        END IF;

        RETURN v1;
    END;
    

    ------------------------------------------------------------------------------
    PROCEDURE DeleteRegistros IS
    BEGIN
        DELETE FROM obr_imp_nota_sefaz_dia_am_capa capa
         WHERE capa.mes = substr(p_DtExclusao, 1, 2)
           AND capa.ano = substr(p_DtExclusao, 4, 4)
           AND capa.estab = p_EstCodigo;

        COMMIT;

        raise_application_error(-20001, 'Registros do periodo Informado foram Deletados com Sucesso.');
    END;

    FUNCTION formataNumero(pValor   NUMBER
                          ,pDecimal NUMBER DEFAULT 2) RETURN VARCHAR2 IS

        mValor VARCHAR2(30);

    BEGIN
        IF pDecimal = 2 THEN
            mValor := rtrim(ltrim(REPLACE(to_char(nvl(pValor, 0), '99999999999999990D99'), ',', '.')));
        ELSIF pDecimal = 3 THEN
            mValor := rtrim(ltrim(REPLACE(to_char(nvl(pValor, 0), '99999999999999990D999'), ',', '.')));
        ELSE
            mValor := pValor;
        END IF;
        RETURN mValor;
    END;

    ------------------------------------------------------------------------------
    --Inicia o processo
    ------------------------------------------------------------------------------
BEGIN

    --syn_out.inicializa(Processo,1000);
    
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' '; --FCP AD.3325264 - 28/08/2024
    
    IF NOT p_synchro4me THEN  --IFA AD 3238780 10/08/2022
       IF p_num_linha = 1 THEN
           syn_xml2000.setIdent(FALSE);
           syn_xml2000.setEncode(FALSE);
           syn_xml2000.setL_feed(FALSE);
           --inicializa na primeira linha
           syn_xml2000.setXml('');
           syn_xml2000.open_tree('?xml', ' version="1.0" encoding="UTF-8" standalone="yes"?');
       END IF;

       IF NOT p_terminou AND p_num_linha > 1 THEN
           syn_xml2000.add_tag('', p_linha_xml);
       END IF;
    END IF;

    IF p_excluir = 'S' AND p_DtExclusao IS NOT NULL THEN
        DeleteRegistros;
    ELSIF p_excluir = 'S' AND p_DtExclusao IS NULL THEN
        Raise_application_error(-20010, 'Favor informar a Data para Exclusão!');
    END IF;

    --IFA AD 3238780 10/08/2022
    --IF p_EstCodigo IS NOT NULL AND p_terminou THEN
    IF ((p_EstCodigo IS NOT NULL AND p_terminou) or p_synchro4me) THEN
        --IFA AD 3238780 10/08/2022
        --Text_xml := syn_xml2000.getClobXml;
        if p_synchro4me then
           Text_xml := p_clob;
        else
           Text_xml := syn_xml2000.getClobXml;
        end if;
        --

        --retiro as tags geradas a mais
        SELECT REPLACE(REPLACE(TEXT_XML, '<>', ''), '</>', '') INTO TEXT_XML FROM DUAL;
        v_Xml := xmltype(Text_xml);
        
        ordem := ordem + 1;

        ie_contribuinte := get_value_tag(v_Xml, '/arquivoNotasDeclararAutodesembaraco/infNotasDeclarar/ieContribuinteDeclarante');
        anoApresentacao := get_value_tag(v_Xml, '/arquivoNotasDeclararAutodesembaraco/infNotasDeclarar/anoApresentacao');
        mesApresentacao := get_value_tag(v_Xml, '/arquivoNotasDeclararAutodesembaraco/infNotasDeclarar/mesApresentacao');

        tXml := v_Xml.extract('/arquivoNotasDeclararAutodesembaraco/infNotasDeclarar/listaNotasFiscais/notaFiscal[position()=' || to_char(ordem) || ']'
                             ,'xmlns="http://www.sefaz.am.gov.br/autodesembaraco"');
                             
                            
        BEGIN

            SELECT mun.uf_codigo
              INTO v_Mun_codigo
              FROM cor_pessoa              pes
                  ,cor_pessoa_vigencia     pvi
                  ,cor_localidade_pessoa   loc
                  ,cor_localidade_vigencia lpv
                  ,cor_municipio           mun
                  ,cor_unidade_federativa  ufe
             WHERE pes.pfj_codigo = p_EstCodigo
               AND pvi.pfj_codigo = pes.pfj_codigo
               
               -- TBT/CBF AD.3221423 07/03/2022
               AND pvi.dt_inicio <= TO_DATE(TO_CHAR('01' || mesApresentacao || anoApresentacao))
               AND (pvi.dt_fim >= TO_DATE(TO_CHAR(CASE
                                                      WHEN mesApresentacao = '02' THEN
                                                       '28'
                                                      WHEN mesApresentacao IN ('01', '03', '05', '07', '08', '10', '12') THEN
                                                       '31'
                                                      WHEN mesApresentacao IN ('04', '06', '09', '11') THEN
                                                       '30'
                                                  END || mesApresentacao || anoApresentacao)) OR pvi.dt_fim IS NULL)
                                                  
               AND loc.pfj_codigo = pvi.pfj_codigo
               AND loc.ind_geral = 'P'
               AND lpv.pfj_codigo = pvi.pfj_codigo
               AND lpv.loc_codigo = loc.loc_codigo
               AND lpv.dt_inicio <= TO_DATE(TO_CHAR('01' || mesApresentacao || anoApresentacao))
               AND (lpv.dt_fim >= TO_DATE(TO_CHAR(CASE
                                                      WHEN mesApresentacao = '02' THEN
                                                       '28'
                                                      WHEN mesApresentacao IN ('01', '03', '05', '07', '08', '10', '12') THEN
                                                       '31'
                                                      WHEN mesApresentacao IN ('04', '06', '09', '11') THEN
                                                       '30'
                                                  END || mesApresentacao || anoApresentacao)) OR lpv.dt_fim IS NULL)
               AND mun.mun_codigo = lpv.mun_codigo
               AND mun.uf_codigo = ufe.uf_codigo;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_Mun_codigo := '';
        END;

        BEGIN

            SELECT DECODE(obr_gerpar.valor, NULL, obr_multi.valor_de, obr_gerpar.valor) valor_De
              INTO vIndice
              FROM obr_obrigacao_fiscal       obr_fis
                  ,obr_obr_parametro          obr_par
                  ,obr_obr_est                obr_est
                  ,obr_obr_gerada             obr_ger
                  ,obr_obrger_parametro       obr_gerpar
                  ,obr_obrger_parametro_multi obr_multi
             WHERE obr_par.obrfis_id = obr_fis.id
               AND obr_est.obrfis_id = obr_fis.id
               AND obr_ger.obrest_id = obr_est.id
               AND obr_gerpar.obrger_id = obr_ger.id
               AND obr_gerpar.obrpar_id = obr_par.id
               AND obr_multi.obrgerpar_id(+) = obr_gerpar.id
               AND obr_fis.sigla = 'DIA-AM'
               AND obr_par.nome = '02 - CODIGO ALTERNATIVO'
               AND obr_est.est_codigo = p_EstCodigo
               AND to_char(obr_ger.dt_inicio_obr, 'MM') = mesApresentacao
               AND to_char(obr_ger.dt_inicio_obr, 'YYYY') = anoApresentacao;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                vIndice := '';
        END;

        WHILE tXml IS NOT NULL
        LOOP
            BEGIN
                chaveNFe := get_value_tag(tXml, '/notaFiscal/chaveNFe');
                numItens := get_value_tag(tXml, '/notaFiscal/numItens');

                iOrdemIDF := 1;

                OPEN cDoc_Fiscais(chaveNFe);
                LOOP
                    FETCH cDoc_Fiscais
                        INTO rDoc_Fiscais;
                    EXIT WHEN cDoc_Fiscais%NOTFOUND;

                    vdoc_fiscais(iOrdemIDF).vCodInternoItem      := rDoc_Fiscais.Merc_Codigo;
                    vdoc_fiscais(iOrdemIDF).vBaseCalculo         := rDoc_Fiscais.Vl_Contabil;
                    vdoc_fiscais(iOrdemIDF).vDtEmissao           := rDoc_Fiscais.Dh_Emissao;
                    vdoc_fiscais(iOrdemIDF).vCFOPCodigo          := rDoc_Fiscais.Cfop_Codigo;
                    vdoc_fiscais(iOrdemIDF).vEmitente_pfj_codigo := rDoc_Fiscais.Emitente_Pfj_Codigo;
                    vdoc_fiscais(iOrdemIDF).vIDFNum              := rDoc_Fiscais.Rownum;
                    vdoc_fiscais(iOrdemIDF).vReconhece           := 'S';
                    vdoc_fiscais(iOrdemIDF).vOmCodigo            := rDoc_Fiscais.om_codigo; --FCP AD.3305766 - 14/03/2024
                    vdoc_fiscais(iOrdemIDF).vVlDifa              := rDoc_Fiscais.vl_difa;   --FCP AD.3305766 - 14/03/2024                    
                    vdoc_fiscais(iOrdemIDF).vIndEntradaSaida     := rDoc_Fiscais.Ind_Entrada_Saida; --FCP AD.3322746 - 08/08/2024
                    vdoc_fiscais(iOrdemIDF).vTipo                := rDoc_Fiscais.Tipo;              --FCP AD.3322746 - 08/08/2024
                    vdoc_fiscais(iOrdemIDF).vTipoComplemento     := rDoc_Fiscais.Tipo_Complemento;  --FCP AD.3322746 - 08/08/2024
                    vdoc_fiscais(iOrdemIDF).vVlStf               := rDoc_Fiscais.Vl_Stf;            --FCP AD.3322746 - 08/08/2024
                    vdoc_fiscais(iOrdemIDF).vVlStt               := rDoc_Fiscais.Vl_Stt;            --FCP AD.3322746 - 08/08/2024
                    vdoc_fiscais(iOrdemIDF).vStcCodigo           := rDoc_Fiscais.Stc_Codigo;        --FCP AD.3322746 - 08/08/2024
                    vdoc_fiscais(iOrdemIDF).vNOPCodigo           := rDoc_Fiscais.Nop_Codigo;        --FCP AD.3322746 - 08/08/2024
                    
                    iOrdemIDF := iOrdemIDF + 1;

                END LOOP;

                IF cDoc_Fiscais%ROWCOUNT = 0 THEN
                    vdoc_fiscais(iOrdemIDF).vDtEmissao := '';
                    vdoc_fiscais(iOrdemIDF).vReconhece := 'N';
                END IF;

                CLOSE cDoc_Fiscais;

                Importado := FALSE;
                BEGIN

                    INSERT INTO OBR_IMP_NOTA_SEFAZ_DIA_AM_CAPA
                        (CHAVE_NFE
                        ,IE
                        ,Ano
                        ,MES
                        ,QTD_ITENS
                        ,Estab
                        ,Dh_Imp
                        ,Reconhece
                        ,Enviado
                        ,Alterado_Em
                        ,Alterado_Por
                        ,Origem
                        ,Dt_emissao)
                    VALUES
                        (chaveNFe
                        ,ie_contribuinte
                        ,anoApresentacao
                        ,mesApresentacao
                        ,numItens
                        ,p_EstCodigo
                        ,SYSDATE
                        ,vdoc_fiscais(1).vReconhece
                        ,'N'
                        ,SYSDATE
                        ,SYN_USERID
                        ,'I'
                        ,vdoc_fiscais(1).vDtEmissao);

                    COMMIT;

                    Imp_Sucesso := Imp_Sucesso + 1;
                    Importado   := TRUE;
                EXCEPTION
                    WHEN OTHERS THEN
                        raise_application_error(-20001, 'Registro CAPA com numero de Chave: ' || chaveNFe || ' - Já Inserido');
                END;

                ordem_item := 0;
                ordem_item := ordem_item + 1;

                WHILE ordem_item <= numItens AND Importado = TRUE
                LOOP
                    BEGIN
                        numItemNFe    := get_value_tag(tXml, '/notaFiscal/produto[position()=' || to_char(ordem_item) || ']/numItemNFe');
                        codProdutoNFe := get_value_tag(tXml, '/notaFiscal/produto[position()=' || to_char(ordem_item) || ']/codProdutoNFe');
                        descProduto   := get_value_tag(tXml, '/notaFiscal/produto[position()=' || to_char(ordem_item) || ']/descProduto');
                        codEAN        := NVL(get_value_tag(tXml, '/notaFiscal/produto[position()=' || to_char(ordem_item) || ']/codEAN'), ' ');
                        codNCM        := NVL(get_value_tag(tXml, '/notaFiscal/produto[position()=' || to_char(ordem_item) || ']/codNCM'), ' ');
                        valorItemNFe  := get_value_tag(tXml, '/notaFiscal/produto[position()=' || to_char(ordem_item) || ']/valorItemNFe');

                        BEGIN

                            SELECT COUNT(*)
                              INTO v_qt_item
                              FROM cor_dof dof
                                  ,cor_idf idf
                             WHERE dof.nfe_localizador = chaveNFe
                               AND dof.dof_sequence = idf.dof_sequence
                               AND dof.codigo_do_site = idf.codigo_do_site
                               AND dof.informante_est_codigo = p_EstCodigo;

                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_qt_item := 0;
                        END;

                        IF v_qt_item >= ordem_item THEN
                            vCodInternoItem := vdoc_fiscais(ordem_item).vCodInternoItem;
                            vBaseCalculo    := vdoc_fiscais(ordem_item).vBaseCalculo;

                            --FCP AD.3322746 - 08/08/2024 - INICIO
                            IF vdoc_fiscais(ordem_item).vIndEntradaSaida = 'E' and vdoc_fiscais(ordem_item).vtipo = 'C' and vdoc_fiscais(ordem_item).vTipoComplemento = 'I' THEN --FCP AD.3312440 - 13/05/2024
                                v_codTipoTributacao := 'N007';                                            
                            ELSIF vdoc_fiscais(ordem_item).vCFOPCodigo = '2.551' AND NVL(vdoc_fiscais(ordem_item).vVLDIFA, 0) = 0 THEN
                                v_codTipoTributacao := 'N015';
                            ELSIF vdoc_fiscais(ordem_item).vCFOPCodigo = '2.551' AND NVL(vdoc_fiscais(ordem_item).vVLDIFA, 0) > 0 THEN
                                v_codTipoTributacao := 'A011';
                            ELSIF vdoc_fiscais(ordem_item).vCFOPCodigo IN ('2.949', '2.907', '2.124', '2.201', '2.920') AND NVL(vdoc_fiscais(ordem_item).vVLDIFA, 0) = 0 THEN
                                v_codTipoTributacao := 'N007';
                            ELSIF vdoc_fiscais(ordem_item).vCFOPCodigo IN ('2.949', '2.556') AND NVL(vdoc_fiscais(ordem_item).vVLDIFA, 0) > 0 THEN
                                v_codTipoTributacao := 'A012';
                            ELSIF vdoc_fiscais(ordem_item).vCFOPCodigo = '2.252' THEN
                                v_codTipoTributacao := 'N010';
                            END IF;
                            --FCP AD.3322746 - 08/08/2024 - FIM

                            IF v_codTipoTributacao IS NULL THEN
                              
                               --FCP AD.3322746 - 08/08/2024 - INICIO
                               IF vdoc_fiscais(ordem_item).vIndEntradaSaida = 'E' AND (vdoc_fiscais(iOrdemIDF).vVlStf > 0 OR vdoc_fiscais(iOrdemIDF).vVlStt > 0) THEN
                                    
                                  BEGIN
                                    select obr_multi.valor_para valor_Para
                                      INTO v_codTipoTributacao
                                      from obr_obrigacao_fiscal           obr_fis, 
                                           obr_oper_dominio               obr_per, 
                                           obr_obr_est                    obr_est,
                                           obr_obr_parametro              obr_par,
                                           obr_obrger_parametro           obr_gerpar,
                                           obr_obrger_parametro_multi     obr_multi,
                                           obr_obr_gerada                 OBR_ger
                                     where obr_fis.sigla      = 'DIA-AM'
                                       and obr_est.est_codigo = p_EstCodigo
                                       and obr_est.obrfis_id  = obr_fis.id
                                       and obr_per.oper_id    = obr_fis.oper_id
                                       and to_char(obr_ger.dt_inicio_obr, 'MM') = mesApresentacao
                                       and to_char(obr_ger.dt_inicio_obr, 'YYYY') = anoApresentacao
                                       and obr_par.obrfis_id  = obr_fis.id
                                       and obr_gerpar.obrpar_id = obr_par.id
                                       and obr_multi.obrgerpar_id(+) = obr_gerpar.id
                                       and obr_per.id           = OBR_ger.opedom_id
                                       and obr_gerpar.obrger_id = obr_ger.id
                                       and obr_per.id = obr_ger.opedom_id
                                       and obr_est.id = obr_ger.obrest_id
                                       and obr_par.nome = '14 - CODIGO DO TIPO DE TRIBUTACAO - SELECAO POR CST'
                                       and DECODE(obr_gerpar.valor, NULL, obr_multi.valor_de, obr_gerpar.valor) = vdoc_fiscais(ordem_item).vStcCodigo;

                                  EXCEPTION
                                      WHEN NO_DATA_FOUND THEN
                                          v_codTipoTributacao := '';
                                  END;                               
                               
                               END IF;
                               --FCP AD.3322746 - 08/08/2024 - FIM
                                                           
                               IF v_codTipoTributacao IS NULL THEN
                                     
                                  BEGIN
                                    select obr_multi.valor_para valor_Para
                                      INTO v_codTipoTributacao 
                                      from obr_obrigacao_fiscal           obr_fis, 
                                           obr_oper_dominio               obr_per, 
                                           obr_obr_est                    obr_est,
                                           obr_obr_parametro              obr_par,
                                           obr_obrger_parametro           obr_gerpar,
                                           obr_obrger_parametro_multi     obr_multi,
                                           obr_obr_gerada                 OBR_ger
                                     where obr_fis.sigla      = 'DIA-AM'
                                       and obr_est.est_codigo = p_EstCodigo
                                       and obr_est.obrfis_id  = obr_fis.id
                                       and obr_per.oper_id    = obr_fis.oper_id
                                       and to_char(obr_ger.dt_inicio_obr, 'MM') = mesApresentacao
                                       and to_char(obr_ger.dt_inicio_obr, 'YYYY') = anoApresentacao
                                       and obr_par.obrfis_id  = obr_fis.id
                                       and obr_gerpar.obrpar_id = obr_par.id
                                       and obr_multi.obrgerpar_id(+) = obr_gerpar.id
                                       and obr_per.id           = OBR_ger.opedom_id
                                       and obr_gerpar.obrger_id = obr_ger.id
                                       and obr_per.id = obr_ger.opedom_id
                                       and obr_est.id = obr_ger.obrest_id
                                       and obr_par.nome = '07 - CODIGO DO TIPO DE TRIBUTACAO - SELECAO POR ITEM'
                                       AND DECODE(obr_gerpar.valor, NULL, obr_multi.valor_de, obr_gerpar.valor) = vdoc_fiscais(ordem_item).vCodInternoItem;

                                  EXCEPTION
                                      WHEN NO_DATA_FOUND THEN
                                          v_codTipoTributacao := '';
                                  END;
                               END IF;   

                               IF v_codTipoTributacao IS NULL THEN

                                  BEGIN

                                    select obr_multi.valor_para valor_Para
                                      INTO v_codTipoTributacao 
                                      from obr_obrigacao_fiscal           obr_fis, 
                                           obr_oper_dominio               obr_per, 
                                           obr_obr_est                    obr_est,
                                           obr_obr_parametro              obr_par,
                                           obr_obrger_parametro           obr_gerpar,
                                           obr_obrger_parametro_multi     obr_multi,
                                           obr_obr_gerada                 OBR_ger
                                     where obr_fis.sigla      = 'DIA-AM'
                                       and obr_est.est_codigo = p_EstCodigo
                                       and obr_est.obrfis_id  = obr_fis.id
                                       and obr_per.oper_id    = obr_fis.oper_id
                                       and to_char(obr_ger.dt_inicio_obr, 'MM') = mesApresentacao
                                       and to_char(obr_ger.dt_inicio_obr, 'YYYY') = anoApresentacao
                                       and obr_par.obrfis_id  = obr_fis.id
                                       and obr_gerpar.obrpar_id = obr_par.id
                                       and obr_multi.obrgerpar_id(+) = obr_gerpar.id
                                       and obr_per.id           = OBR_ger.opedom_id
                                       and obr_gerpar.obrger_id = obr_ger.id
                                       and obr_per.id = obr_ger.opedom_id
                                       and obr_est.id = obr_ger.obrest_id
                                       and obr_par.nome = '03 - CODIGO DO TIPO DE TRIBUTACAO - SELECAO POR CFOP'
                                       AND DECODE(obr_gerpar.valor, NULL, obr_multi.valor_de, obr_gerpar.valor) = vdoc_fiscais(ordem_item).vCFOPCodigo;
                                       


                                  EXCEPTION
                                      WHEN NO_DATA_FOUND THEN
                                          v_codTipoTributacao := '';
                                  END;

                               END IF;
                               
                               --FCP AD.3322746 - 08/08/2024 - INICIO
                               IF v_codTipoTributacao IS NULL THEN

                                  BEGIN

                                    select obr_multi.valor_para valor_Para
                                      INTO v_codTipoTributacao 
                                      from obr_obrigacao_fiscal           obr_fis, 
                                           obr_oper_dominio               obr_per, 
                                           obr_obr_est                    obr_est,
                                           obr_obr_parametro              obr_par,
                                           obr_obrger_parametro           obr_gerpar,
                                           obr_obrger_parametro_multi     obr_multi,
                                           obr_obr_gerada                 OBR_ger
                                     where obr_fis.sigla      = 'DIA-AM'
                                       and obr_est.est_codigo = p_EstCodigo
                                       and obr_est.obrfis_id  = obr_fis.id
                                       and obr_per.oper_id    = obr_fis.oper_id
                                       and to_char(obr_ger.dt_inicio_obr, 'MM') = mesApresentacao
                                       and to_char(obr_ger.dt_inicio_obr, 'YYYY') = anoApresentacao
                                       and obr_par.obrfis_id  = obr_fis.id
                                       and obr_gerpar.obrpar_id = obr_par.id
                                       and obr_multi.obrgerpar_id(+) = obr_gerpar.id
                                       and obr_per.id           = OBR_ger.opedom_id
                                       and obr_gerpar.obrger_id = obr_ger.id
                                       and obr_per.id = obr_ger.opedom_id
                                       and obr_est.id = obr_ger.obrest_id
                                       and obr_par.nome = '13 - CODIGO DO TIPO DE TRIBUTACAO - SELECAO POR CFOP X NOP'
                                       AND DECODE(obr_gerpar.valor, NULL, obr_multi.valor_de, obr_gerpar.valor) = vdoc_fiscais(ordem_item).vCFOPCodigo || '-' || vdoc_fiscais(ordem_item).vNOPCodigo;

                                  EXCEPTION
                                      WHEN NO_DATA_FOUND THEN
                                          v_codTipoTributacao := '';
                                  END;

                               END IF;  
                               --FCP AD.3322746 - 08/08/2024 - FIM                             
                               
                            END IF;    

                            v_Uf_Codigo_Emitente := '';

                            BEGIN
                                SELECT mun.uf_codigo
                                  INTO v_Uf_Codigo_Emitente
                                  FROM cor_pessoa              pes
                                      ,cor_pessoa_vigencia     pvi
                                      ,cor_localidade_pessoa   loc
                                      ,cor_localidade_vigencia lpv
                                      ,cor_municipio           mun
                                      ,cor_unidade_federativa  ufe
                                 WHERE pes.pfj_codigo = vdoc_fiscais(ordem_item).vEmitente_pfj_codigo
                                   AND pvi.pfj_codigo = pes.pfj_codigo
                                   AND pvi.dt_inicio <= TO_DATE(TO_CHAR('01' || mesApresentacao || anoApresentacao))
                                   AND (pvi.dt_fim >= TO_DATE(TO_CHAR(CASE
                                                                          WHEN mesApresentacao = '02' THEN
                                                                           '28'
                                                                          WHEN mesApresentacao IN ('01', '03', '05', '07', '08', '10', '12') THEN
                                                                           '31'
                                                                          WHEN mesApresentacao IN ('04', '06', '09', '11') THEN
                                                                           '30'
                                                                      END || mesApresentacao || anoApresentacao)) OR pvi.dt_fim IS NULL)
                                   AND loc.pfj_codigo = pvi.pfj_codigo
                                   AND loc.ind_geral = 'P'
                                   AND lpv.pfj_codigo = pvi.pfj_codigo
                                   AND lpv.loc_codigo = loc.loc_codigo
                                   AND lpv.dt_inicio <= TO_DATE(TO_CHAR('01' || mesApresentacao || anoApresentacao))
                                   AND (lpv.dt_fim >= TO_DATE(TO_CHAR(CASE
                                                                          WHEN mesApresentacao = '02' THEN
                                                                           '28'
                                                                          WHEN mesApresentacao IN ('01', '03', '05', '07', '08', '10', '12') THEN
                                                                           '31'
                                                                          WHEN mesApresentacao IN ('04', '06', '09', '11') THEN
                                                                           '30'
                                                                      END || mesApresentacao || anoApresentacao)) OR lpv.dt_fim IS NULL)
                                   AND mun.mun_codigo = lpv.mun_codigo
                                   AND mun.uf_codigo = ufe.uf_codigo;

                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    v_Uf_Codigo_Emitente := '';
                            END;

                            --FCP AD.3295866 - 02/01/2024 - FIM
                            /*IF v_mun_codigo IN ('1302603', '1303536', '1303569', '1304062') AND v_Uf_Codigo_Emitente IN ('PR', 'SC', 'RS', 'SP', 'MG', 'ES', 'RJ') THEN
                                v_Lista_tipo := 'ZFM_S_SE';
                            ELSIF v_mun_codigo IN ('1302603', '1303536', '1303569', '1304062') THEN
                                v_Lista_tipo := 'ZFM_OUTROS';
                            ELSIF v_Uf_Codigo_Emitente IN ('PR', 'SC', 'RS', 'SP', 'MG', 'ES', 'RJ') THEN
                                v_Lista_tipo := 'AMAZONAS_S_SE';
                            ELSE
                                v_Lista_tipo := 'AMAZONAS_OUTROS';
                            END IF;*/

                            --FCP AD.3305766 - 14/03/2024
                            IF vdoc_fiscais(ordem_item).vOmCodigo IS NULL THEN
                            
                              BEGIN
                                
                                SELECT merc.dflt_om_codigo
                                INTO   vdoc_fiscais(ordem_item).vOmCodigo
                                FROM   cor_mercadoria  merc
                                WHERE  merc.merc_codigo = vCodInternoItem ;     

                              EXCEPTION WHEN NO_DATA_FOUND THEN
                                vdoc_fiscais(ordem_item).vOmCodigo := '';
                              END; 
                            END IF;   
/*                            
                            IF v_mun_codigo NOT IN ('1302603', '1303536', '1303569', '1304062') THEN
                               IF v_om_codigo IN ('1', '2', '3', '8') THEN
                                  v_Lista_tipo := 'AMAZONAS_TODOS';  
                               ELSIF v_Uf_Codigo_Emitente IN ('AC', 'AP', 'PA', 'RO', 'RR', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'GO', 'MT', 'MS', 'DF') THEN
                                  v_Lista_tipo := 'AMAZONAS_N_NE_CO';
                               ELSIF v_Uf_Codigo_Emitente IN ('PR', 'SC', 'RS', 'ES', 'MG', 'RJ', 'SP') THEN
                                  v_Lista_tipo := 'AMAZONAS_S_SE';     
                               END IF;
                            ELSE 
                               IF v_om_codigo IN ('1', '2', '3', '8') THEN
                                  v_Lista_tipo := 'ZFM_TODOS';  
                               ELSIF v_Uf_Codigo_Emitente IN ('AC', 'AP', 'PA', 'RO', 'RR', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'GO', 'MT', 'MS', 'DF') THEN
                                  v_Lista_tipo := 'ZFM_N_NE_CO';
                               ELSIF v_Uf_Codigo_Emitente IN ('PR', 'SC', 'RS', 'ES', 'MG', 'RJ', 'SP') THEN
                                  v_Lista_tipo := 'ZFM_S_SE';     
                               END IF;                      

                            END IF;*/
                            
                            --FCP AD.3295866 - 02/01/2024 - FIM                            

                            --FCP AD.3305766 - 15/03/2024 - INICIO
                            IF v_mun_codigo NOT IN ('1302603', '1303536', '1303569', '1304062') THEN
                               IF vdoc_fiscais(ordem_item).vOmCodigo IN ('1', '2', '3', '6', '7') THEN
                                 v_Lista_tipo := 'AMAZONAS_TODOS';  
                                 IF v_codTipoTributacao IS NULL THEN
                                    v_codTipoTributacao := 'A031';
                                 END IF;   
                               ELSIF vdoc_fiscais(ordem_item).vOmCodigo IN ('0', '4', '5', '8') THEN   
                                 IF v_codTipoTributacao IS NULL THEN    
                                    v_codTipoTributacao := 'N004';  
                                 END IF;     
                                 IF v_Uf_Codigo_Emitente IN ('AC', 'AP', 'PA', 'RO', 'RR', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'GO', 'MT', 'MS', 'DF') THEN
                                    v_Lista_tipo := 'AMAZONAS_N_NE_CO';
                                 ELSIF v_Uf_Codigo_Emitente IN ('PR', 'SC', 'RS', 'ES', 'MG', 'RJ', 'SP') THEN
                                    v_Lista_tipo := 'AMAZONAS_S_SE';     
                                 END IF;
                               END IF;  
                            ELSE 
                               IF vdoc_fiscais(ordem_item).vOmCodigo IN ('1', '2', '3', '6', '7') THEN
                                 v_Lista_tipo := 'ZFM_TODOS';  
                                 IF v_codTipoTributacao IS NULL THEN
                                    v_codTipoTributacao := 'A031';
                                 END IF;
                               ELSIF vdoc_fiscais(ordem_item).vOmCodigo IN ('0', '4', '5', '8') THEN  
                                 IF v_codTipoTributacao IS NULL THEN        
                                    v_codTipoTributacao := 'N004';
                                 END IF;   
                                 IF v_Uf_Codigo_Emitente IN ('AC', 'AP', 'PA', 'RO', 'RR', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA', 'GO', 'MT', 'MS', 'DF') THEN
                                    v_Lista_tipo := 'ZFM_N_NE_CO';
                                 ELSIF v_Uf_Codigo_Emitente IN ('PR', 'SC', 'RS', 'ES', 'MG', 'RJ', 'SP') THEN
                                    v_Lista_tipo := 'ZFM_S_SE';     
                                 END IF;       
                               END IF;                 
                            END IF;                
                            
                            --FCP AD.3322746 - 08/08/2024 - trecho comentado pois foi colocado acima, com mais condicoes
                            /*IF vdoc_fiscais(ordem_item).vCFOPCodigo = '2.551' AND NVL(vdoc_fiscais(ordem_item).vVLDIFA, 0) = 0 THEN
                               codTipoTributacao := 'N015';
                            ELSIF vdoc_fiscais(ordem_item).vCFOPCodigo = '2.551' AND NVL(vdoc_fiscais(ordem_item).vVLDIFA, 0) > 0 THEN     
                               codTipoTributacao := 'A011';
                            ELSIF vdoc_fiscais(ordem_item).vCFOPCodigo in ('2.949', '2.907', '2.124', '2.201', '2.920') AND NVL(vdoc_fiscais(ordem_item).vVLDIFA, 0) = 0 THEN 
                               codTipoTributacao := 'N007';
                            ELSIF vdoc_fiscais(ordem_item).vCFOPCodigo in ('2.949', '2.556') AND NVL(vdoc_fiscais(ordem_item).vVLDIFA, 0) > 0 THEN  
                               codTipoTributacao := 'A012';
                            END IF;*/   
                            --FCP AD.3305766 - 14/03/2024 - INICIO

                            BEGIN
                                SELECT --REPLACE(lista.descricao, ',', '.')
                                 lista.descricao / 100
                                  INTO v_vl_Multiplicador
                                  FROM Syn_Lista_Valores lista
                                 WHERE lista.lista_valor = v_codTipoTributacao
                                   AND lista.lista_tipo = v_Lista_tipo;
                            EXCEPTION
                                WHEN no_data_found THEN
                                    v_vl_Multiplicador := '0';
                            END;

                            IF TO_NUMBER(v_vl_Multiplicador) > 0 AND vBaseCalculo > 0 THEN
                                v_valor_imposto := nvl(vBaseCalculo, 0) * nvl(v_vl_Multiplicador, 0);
                            END IF;

                        ELSE
                            vCodInternoItem    := '';
                            vBaseCalculo       := 0;
                            v_codTipoTributacao  := '';
                            v_vl_Multiplicador := '0';
                            v_valor_imposto    := '0';
                        END IF;

                        BEGIN
                            INSERT INTO OBR_IMP_NOTA_SEFAZ_DIA_AM_ITEM
                                (chave_NFe
                                ,num_Item_NFe
                                ,cod_Produto
                                ,desc_Produto
                                ,cod_EAN
                                ,cod_NCM
                                ,vl_Item
                                ,vl_base_item
                                ,cod_interno_item
                                ,cod_tipo_tributacao
                                ,vl_multiplicador
                                ,vl_imposto
                                ,indice
                                ,cst)
                            VALUES
                                (chaveNFe
                                ,numItemNFe
                                ,codProdutoNFe
                                ,descProduto
                                ,codEAN
                                ,codNCM
                                ,nvl(valorItemNFe, '0')
                                ,nvl(vBaseCalculo, 0)
                                ,vCodInternoItem
                                ,v_codTipoTributacao
                                ,TO_NUMBER(v_vl_Multiplicador) * 100
                                ,v_valor_imposto
                                ,vIndice
                                ,cst);

                            COMMIT;

                        EXCEPTION
                            WHEN dup_val_on_index THEN
                                raise_application_error(-20001, 'Item com numero de Chave: ' || chaveNFe || ' - Já Inserido');
                        END;

                        ordem_item      := ordem_item + 1;
                        v_valor_imposto := 0;
                    END;
                END LOOP;

                ordem := ordem + 1;

                tXml := v_Xml.extract('/arquivoNotasDeclararAutodesembaraco/infNotasDeclarar/listaNotasFiscais/notaFiscal[position()=' || to_char(ordem) || ']'
                                     ,'xmlns="http://www.sefaz.am.gov.br/autodesembaraco"');

                vdoc_fiscais := vdoc_fiscais_Limpar;

            END;
        END LOOP;

        raise_application_error(-20001, chr(13) || 'Foram importados ' || Imp_Sucesso || ' Notas com Sucesso!');

    END IF;

END;
/
