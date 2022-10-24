import graphlib
import threading
import paho.mqtt.client as mqtt
import os
import argparse

# Leitura de valores da linha de comando
parser = argparse.ArgumentParser(description='Simula fábrica da cadeia de produção')
parser.add_argument('-n', '--num',  type=int, help='Número da fábrica',                    required=True)
parser.add_argument('-p','--prod', nargs='+', help='Lista de produtos produzidos pela fábrica', required=True)
argumentos = parser.parse_args()

# Variáveis globais
nome_usuario = "Fábrica " + str(argumentos.num)
produtos = list(map(int, argumentos.prod))
topico = "Reabastecimento(produto)"

def on_connect(client, userdata, flags, rc):
    """
    Chamado quando o broker responde a solicitação de conexão.
    """
    # Limpa o terminal
    os.system('cls' if os.name == 'nt' else 'clear')
    # Printa na tela
    print(nome_usuario + " se conectou ao tópico " + topico)
    # Se perdermos a conexão e reconectar as assinaturas serão renovadas
    client.subscribe(topico)

def on_message(client, userdata, msg):
    """
    Chamado quando uma mensagem foi recebida em um tópico que o cliente assina.
    """
    # Decodifica a mensagem
    mensagem_entrada = msg.payload.decode()
    mensagem_separada = [x.strip() for x in mensagem_entrada.split(',',1)]
    remetente = mensagem_separada[0]

    # Se recebeu uma mensagem do centro de distribuição
    if remetente == "Centro Distribuição":

        mensagem_separada = mensagem_separada[1].split()
        id_produto = int(mensagem_separada[1])
        qtd_produto = mensagem_separada[3]

        if id_produto in produtos:
            print(remetente + " requisitou reabastecimento para {}: {} produtos {}".format(nome_usuario, qtd_produto,id_produto))

            client.publish(topico, nome_usuario + "," + "Crédito " +
                "Produto " + str(id_produto) + " Quantidade " + qtd_produto)
    
    elif remetente == "noticia":
        print(mensagem_separada[1])

def subscribe():
    """
    Inscreve o cliente em um tópico chamando os métodos
    de conexão e de mensagens.
    """
    client.publish(topico,"noticia" + "," + nome_usuario + " entrou neste tópico") 
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_forever()

if __name__ == "__main__":
    client = mqtt.Client()
    client.connect("broker.hivemq.com",1883,60)
    thr_sub = threading.Thread(target=subscribe)
    thr_sub.start()