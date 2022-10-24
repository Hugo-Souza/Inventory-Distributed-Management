import graphlib
import threading
import paho.mqtt.client as mqtt
import os
import random
from tabulate import tabulate
import pandas as pd
import uuid
import numpy as np
import time
import argparse

# Leitura de valores da linha de comando
parser = argparse.ArgumentParser(description='Simula loja da cadeia de produção')
parser.add_argument('-n', '--num',  type=int, help='Número da loja', required=True)
argumentos = parser.parse_args()

# Variáveis globais
contador_clientes = 0
nome_loja = "Loja " + str(argumentos.num)
topico = "Repo"
MAXIMO_ESTOQUE = 200

# Lê estoque de arquivo .csv e salva em lista
estoque = pd.read_csv('estoque_loja.csv', delimiter=',', index_col=0) 


def imprimir_estoque():
    """
    Imprime o DataFrame estoque como tabela
    """
    print(tabulate(estoque, headers = 'keys'))


def debito_estoque(index_produto, quantidade_produto):
    """Retira uma quantiadade de produtos do estoque
    Parâmetros:
        index_produto: Número do produto a ser retirado do estoque
        quantidade_produto: Quantidade de elementos do produto a ser retirado do estoque
    """
    global estoque

    quantidade_antiga = estoque["Quantidade"].values[index_produto]

    estoque.loc[index_produto,["Quantidade","Porcentagem"]] = [quantidade_antiga - quantidade_produto, (quantidade_antiga - quantidade_produto)/MAXIMO_ESTOQUE * 100]

def credito_estoque(index_produto, quantidade_produto):
    """Adiciona uma quantiadade de produtos do estoque
    Parâmetros:
        index_produto: Número do produto a ser retirado do estoque
        quantidade_produto: Quantidade de elementos do produto a ser retirado do estoque
    """
    global estoque

    quantidade_antiga = estoque["Quantidade"].values[index_produto]

    estoque.loc[index_produto,["Quantidade","Porcentagem"]] = [quantidade_antiga + quantidade_produto, (quantidade_antiga + quantidade_produto)/MAXIMO_ESTOQUE * 100]


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
        (estoque['Porcentagem'] >= 0)  & (estoque['Porcentagem'] < 25)]

    # Lista com o valor atribuído a cada condição
    values = ['Verde', 'Amarelo', 'Vermelho']

    # Atualiza a coluna cor os valores
    estoque['Cor'] = np.select(conditions, values)


def clientes():
    quantidade_produtos = random.randint(2, 5)
    
    # Aleatoriza quais foram os produtos comprados pelos clientes
    produtos_comprados = []
    for i in range(quantidade_produtos):
        produtos_comprados.append(random.randint(0, 199))
    
    # Aleatoriza quais foram as quantidades comprados pelos clientes
    quantidade_comprados = []
    for i in range(quantidade_produtos):
        quantidade_comprados.append(random.randint(10, 20))

    # Realiza o débito do estoque
    for i in range(quantidade_produtos):
        debito_estoque(produtos_comprados[i], quantidade_comprados[i])

    # Exibe os dados da compra do cliente
    global contador_clientes

    print("O cliente {} comprou {} produtos da {}: ".format(contador_clientes, quantidade_produtos, nome_loja))
    contador_clientes += 1

    for i in range(quantidade_produtos):
        print("\t{} produtos com ID {}".format(quantidade_comprados[i], produtos_comprados[i]))

######################################################################################
#                           Funções Pub/Sub                                          #
######################################################################################

def on_connect(client, userdata, flags, rc):
    os.system('cls' if os.name == 'nt' else 'clear')
    print(nome_loja + " conectada ao tópico " + topico)
    imprimir_estoque()
    client.subscribe(topico)


def on_message(client, userdata, msg):
    global estoque
    mensagem_entrada = msg.payload.decode()
    mensagem_separada = [x.strip() for x in mensagem_entrada.split(',',1)]
    remetente = mensagem_separada[0]

    # Se Loja recebeu mensagem de crédito
    # do Centro de Distribuição, realiza operação de crédito
    if remetente == "Centro Distribuição":
        loja_num = mensagem_separada[1].split(' ')[-1]
        
        if nome_loja == 'Loja ' + str(loja_num):
            print("{} -> {}: {}".format(remetente, nome_loja, mensagem_separada[1]))

        # Obtém dados do produto
        dados_produto = mensagem_separada[1].split()
        id_produto = int(dados_produto[2])
        qtd_produto = int(dados_produto[4])

        # Realiza operação de crédito no estoque
        #imprimir_estoque()
        credito_estoque(id_produto, qtd_produto)
        atualizar_cores()
        #print(id_produto, qtd_produto)
        #imprimir_estoque()

    elif remetente == "noticia":
        print(mensagem_separada[1])


def publish():
    """
    
    """
    # Simula a compra aleatória de produtos por clientes
    clientes()

    # Atualiza as cores do DataFrame
    atualizar_cores()
    imprimir_estoque()

    # Atualiza o arquivo .csv
    #estoque.to_csv('estoque.csv', index=True)

    # Lista de produtos com estoque na cor vermelha
    produtos_no_vermelho = list(estoque[estoque["Cor"] == "Vermelho"].index)
    # Quantidade necessária para que produtos no vermelho encham o estoque
    quantidade_produtos_no_vermelho = list(MAXIMO_ESTOQUE - estoque[estoque["Cor"] == "Vermelho"].Quantidade)

    # Se existem produtos no vermelho, envia mensagem no tópico 
    # reposição para que o centro de distribuição envie produtos 
    # para o estoque, completando o estoque
    if (len(produtos_no_vermelho)>0):
        for i in range(len(produtos_no_vermelho)):
            mensagem_publicada = "Repor Produto {} Quantidade {}".format(produtos_no_vermelho[i],quantidade_produtos_no_vermelho[i])
            client.publish(topico, nome_loja + "," + mensagem_publicada)

    time.sleep(5)

    return publish()


def subscribe():

    client.publish(topico, "noticia" + "," + nome_loja + " entrou neste tópico") 
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_forever()


if __name__ == '__main__':
    """
    Função Principal
    """
    client = mqtt.Client()
    client.connect("broker.hivemq.com",1883,60)
    thr_pub = threading.Thread(target=publish)
    thr_sub = threading.Thread(target=subscribe)
    thr_pub.start()
    thr_sub.start()