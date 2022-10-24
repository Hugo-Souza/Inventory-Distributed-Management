import graphlib
import threading
import paho.mqtt.client as mqtt
import os
import pandas as pd
import numpy as np
from time import sleep
from tabulate import tabulate

# Variáveis globais
nome_usuario = "Centro Distribuição"
#topico = "Reabastecimento(produto)"
topico_loja = "Repo"
topico = [("Reabastecimento(produto)", 0), ("Repo", 0)]
MAXIMO_ESTOQUE = 200
estoque = pd.read_csv('estoque_cd.csv', delimiter=',', index_col=0) 

# Métodos
def imprimir_estoque():
    """
    Imprime o DataFrame estoque como tabela
    """
    print(tabulate(estoque, headers = 'keys'))

def credito_estoque(index_produto, quantidade_produto):
    """Adiciona uma quantiadade de produtos do estoque

    Parâmetros:
        index_produto: Número do produto a ser retirado do estoque
        quantidade_produto: Quantidade de elementos do produto a ser retirado do estoque

    """
    global estoque

    quantidade_antiga = estoque["Quantidade"].values[index_produto]

    estoque.loc[index_produto,["Quantidade","Porcentagem"]] = [quantidade_antiga + quantidade_produto, (quantidade_antiga + quantidade_produto)/MAXIMO_ESTOQUE * 100]

def debito_estoque(index_produto, quantidade_produto):
    """Retira uma quantiadade de produtos do estoque

    Parâmetros:
        index_produto: Número do produto a ser retirado do estoque
        quantidade_produto: Quantidade de elementos do produto a ser retirado do estoque

    """
    global estoque

    quantidade_antiga = estoque["Quantidade"].values[index_produto]

    estoque.loc[index_produto,["Quantidade","Porcentagem"]] = [quantidade_antiga - quantidade_produto, (quantidade_antiga - quantidade_produto)/MAXIMO_ESTOQUE * 100]

def on_connect(client, userdata, flags, rc):
    """
    Chamado quando o broker responde a solicitação de conexão.
    """
    # Limpa o terminal
    os.system('cls' if os.name == 'nt' else 'clear')
    # Printa na tela
    print("{} se conectou aos tópicos {} e {}".format(nome_usuario, topico[0][0], topico[1][0]))
    # Se perdermos a conexão e reconectar as assinaturas serão renovadas
    client.subscribe(topico)

def atualizar_cores():
    """
    Atualiza a classificação da cor dos produtos
    em estoque com base em suas porcentagens
    """
    global estoque

    # Cria uma lista com as condições
    conditions = [
        (estoque['Porcentagem'] >= 50),
        (estoque['Porcentagem'] >= 25) & (estoque['Porcentagem'] < 50),
        (estoque['Porcentagem'] >= 0) & (estoque['Porcentagem'] < 25)]

    # Lista com o valor atribuído a cada condição
    values = ['Verde', 'Amarelo', 'Vermelho']

    # Atualiza a coluna cor os valores
    estoque['Cor'] = np.select(conditions, values)

def on_message(client, userdata, msg):
    """
    Chamado quando uma mensagem foi recebida em um tópico que o cliente assina.
    """
    # Decodifica a mensagem
    mensagem_entrada = msg.payload.decode()
    mensagem_separada = [x.strip() for x in mensagem_entrada.split(',',1)]
    remetente = mensagem_separada[0]

    # Se Centro de Distribuição recebeu mensagem de crédito
    # de alguma fábrica, realiza operação de crédito
    if "Fábrica" in remetente:
        print("{} -> {}: {}".format(remetente, nome_usuario, mensagem_separada[1]))

        # Obtém dados do produto
        dados_produto = mensagem_separada[1].split()
        id_produto = int(dados_produto[2])
        qtd_produto = int(dados_produto[4])

        # Realiza operação de crédito no estoque
        credito_estoque(id_produto, qtd_produto)
        atualizar_cores()

    # Se Centro de Distribuição recebeu mensagem de
    # alguma loja, realiza operação de débito e publica
    # no tópico das lojas
    if "Loja" in remetente:
        print("{} -> {}: {}".format(remetente, nome_usuario, mensagem_separada[1]))

        # Obtém dados do produto
        dados_produto = mensagem_separada[1].split()
        id_produto = int(dados_produto[2])
        qtd_produto = int(dados_produto[4])
        
        # Realiza operação de dédito no estoque
        #imprimir_estoque()
        debito_estoque(id_produto, qtd_produto)
        atualizar_cores()
        #imprimir_estoque()

        # Publica no tópico da loja para realizar abastecimento
        mensagem_publicada = "Reposto Produto {} Quantidade {} na {}".format(id_produto,qtd_produto,remetente)
        client.publish(topico_loja, nome_usuario + "," + mensagem_publicada)

    # Noticia se alguém se conectou ao tópico
    elif remetente == "noticia":
        print(mensagem_separada[1])

def publish():
    #nova = input()

    # Atualiza classificação de cores do estoque
    atualizar_cores()

    # Lista de produtos com estoque na cor vermelha
    produtos_no_vermelho = list(estoque[estoque["Cor"] == "Vermelho"].index)
    # Quantidade necessária para que produtos no vermelho encham o estoque
    quantidade_produtos_no_vermelho = list(MAXIMO_ESTOQUE - estoque[estoque["Cor"] == "Vermelho"].Quantidade)

    # Se existem produtos no vermelho, envia mensagem no tópico 
    # reabastecimento para que as fábricas que produzem tais produtos
    # envie produtos para completar o estoque do CD
    if (len(produtos_no_vermelho)>0):
        for i in range(len(produtos_no_vermelho)):
            mensagem_publicada = "Produto {} Quantidade {}".format(produtos_no_vermelho[i],quantidade_produtos_no_vermelho[i])
            client.publish(topico[0][0], nome_usuario + "," + mensagem_publicada)

    sleep(5)
    return publish()

def subscribe():
    """
    Inscreve o cliente em um tópico chamando os métodos
    de conexão e de mensagens.
    """
    client.publish(topico[0][0],"noticia" + "," + nome_usuario + " entrou neste tópico")
    client.publish(topico[1][0],"noticia" + "," + nome_usuario + " entrou neste tópico")  
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_forever()

if __name__ == "__main__":
    """
    Função Principal
    """
    client = mqtt.Client()
    client.connect("broker.hivemq.com",1883,60)
    thr_sub = threading.Thread(target=subscribe)
    thr_pub = threading.Thread(target=publish)
    thr_sub.start()
    thr_pub.start()
