# -*- coding: utf-8 -*-
import pandas as pd
import timeit
import json
import couchdb
from pymongo import MongoClient


class AnalisadorBD:

    '''
        função init recebe como parametro o path do arquivo
        contendo os dados a serem utilizados no código
    '''

    def __init__(self, path):
        self._arquivo_json = self.ler_json(path)
        self._conexao_couchdb = ''
        self._conexao_mongodb = ''

    '''
        :func abrir_conexao_mongodb
            - Abre conexão com banco de dados mongo
        
        :parameter: 
            - host - o host ao qual o collection de dados está
            - port - porta associada ao banco de dados
    '''

    def abrir_conexao_mongodb(self, host=None, port=None, ):
        # abre conexão com collection de dados mongodb
        self._conexao_mongo = MongoClient(host=host, port=port)

    '''
        :func abrir_conexao_couchdb
            - Abre conexão com o banco de dados couch

        :parameters: 
            user - usuario com permissão de acesso ao banco de dados
            password - senha do usuário 
    '''

    def abrir_conexao_couchdb(self, user, password):
        self._conexao_couchdb = couchdb.Server("http://%s:%s@localhost:5984/" % (user, password))

    '''
        :func ler_json
            - Ler os dados dos documentos a serem utilizados

        :parameters: 
            path - caminho onde o arquivo está armazenado
        Retorno:
            serie pandas  
    '''

    def ler_json(self, path):
        return pd.read_json(path, typ='series', orient='table', lines=True)

    '''
        :func insert_documents_mongodb
            - Insere  documentos no mongodb

        :parameters: 
            n - inteiro representado quantidade de documentos a serem inseridos   
    '''

    def insert_documents_mongodb(self, n=0):
        # recupera collection do banco
        collection = self._conexao_mongo['diplomas']
        # deleta todos os documentos salvos na collection
        collection.diplomas.delete_many({})
        # inicia a contagem de tempo
        inicio_mongo = timeit.default_timer()
        i = 1
        # percorre os 'n' documentos a serem inseridos
        for x in self._arquivo_json:
            # verifica se chegou a quantidade de documentos
            if i <= n:
                collection.diplomas.insert_one(x)
                i += 1
            # sai do laço
            else:
                break
        # encerra contagem de tempo
        fim_mongo = timeit.default_timer()
        # apresenta o resultado
        print("========================================================================")
        print('INSERANDO %i DOCUMENTOS NO MONGODB' % n)
        print('DURACÃO: %f' % (fim_mongo - inicio_mongo))
        print("========================================================================")

    '''
        :func insert_documents_couchdb
            - Insere  documentos no couchdb

        :parameters: 
            n - inteiro representado quantidade de documentos a serem inseridos
    '''

    def insert_documents_couchdb(self, n=0):
        # remove a base de diplomas salvo
        self._conexao_couchdb.delete('diplomas')
        # cria a base de dados diplomas
        collection = self._conexao_couchdb.create('diplomas')
        # inicia contagem de tempo
        inicio_couchdb = timeit.default_timer()
        i = 1
        # percorre os 'n' documentos a serem inseridos
        for x in self._arquivo_json.values:
            # verifica se chegou a quantidade de documentos
            if i <= n:
                collection.save(x)
                i += 1
            # sai do laço
            else:
                break
        # encerra contagem de tempo
        fim_couchdb = timeit.default_timer()
        # apresenta o resultado
        print("========================================================================")
        print('INSERANDO %i DOCUMENTOS NO COUCHDB' % n)
        print('DURACÃO: %f' % (fim_couchdb - inicio_couchdb))
        print("========================================================================")

    '''
        :func remove_documents_mongodb
            - remove  documentos no mongodb

        :parameters: 
            n - inteiro representado quantidade de documentos a serem removidos
    '''

    def remove_documents_mongodb(self, n):
        # recupera collection diplomas
        collection = self._conexao_mongo['diplomas']
        query = {"_id": {'$gte': "0"}}
        # inicia contagem de tempo
        inicio_mongo = timeit.default_timer()
        # percorre até 0 até n
        for x in range(n):
            # remove documento
            collection.diplomas.delete_one(query)
        # encerra contagem de tempo
        fim_mongo = timeit.default_timer()
        # apresenta resultado
        print("========================================================================")
        print('REMOVENDO %i DOCUMENTO(S) NO MONGODB' % n)
        print('DURACÃO: %f' % (fim_mongo - inicio_mongo))
        print("========================================================================")

    '''
        :func remove_documents_couchdb
            - remove  documentos no mongodb

        :parameters: 
            n - inteiro representado quantidade de documentos a serem removidos
    '''

    def remove_documents_couchdb(self, n=0):
        # recupera base
        bd = self._conexao_couchdb['diplomas']
        # recupera todos os arquivos
        list_documents = list(bd.view('_all_docs', include_docs=True))
        # inicia contagem de tempo
        inicio_couchdb = timeit.default_timer()
        # percorre de 0 até n
        for i in range(n):
            # deleta arquivo
            bd.delete(list_documents[i]['doc'])
        # encerra contagem de tempo
        fim_couchdb = timeit.default_timer()
        # apresenta resultado
        print("========================================================================")
        print('REMOVENDO %i DOCUMENTO(S) NO COUCHDB' % n)
        print('DURACÃO: %f' % (fim_couchdb - inicio_couchdb))
        print("========================================================================")

    '''
        :func update_documents_mongodb
            - atualiza  documentos no mongodb

        :parameters: 
            n - inteiro representado quantidade de documentos a serem atualizados
    '''

    def update_documents_mongodb(self, n=0):
        # recupera base
        collection = self._conexao_mongo['diplomas']
        # inicia contagem de tempo
        inicio_mongo = timeit.default_timer()
        # lista temporaria
        list_ids = []
        # recupera os n primeiros documentos
        ids = collection.diplomas.find().limit(n)
        # adiciona os ids na lista temporária
        for x in ids:
            list_ids.append(x['_id'])
        # atualiza os documentos dos ids da lista temporária
        collection.diplomas.update_many(
            {
                '_id': {'$in': list_ids}
            },
            {"$set": {'curso': 'MECATRÔNICA', 'livro': 'P'}
             }
        )
        # encerra contagem de tempo
        fim_mongo = timeit.default_timer()
        # apresenta resultado
        print("========================================================================")
        print('ATUALIZANDO %i DOCUMENTO(S) NO MONGODB' % n)
        print('DURACÃO: %f' % (fim_mongo - inicio_mongo))
        print("========================================================================")

    '''
        :func update_documents_couchdb
            - atualiza  documentos no couchdb

        :parameters: 
            n - inteiro representado quantidade de documentos a serem atualizados
    '''

    def update_documents_couchdb(self, n=0):
        # recupera base
        collection = self._conexao_couchdb['diplomas']
        # recupera todos os documentos
        list_documents = list(collection.view('_all_docs', include_docs=True))
        # inicia contagem de tempo
        inicio_couchdb = timeit.default_timer()
        list_temp = []
        # adiciona os n documentos na lista temporária
        for i in range(n):
            doc = list_documents[i]['doc']
            doc['curso'] = "MECATRÔNICA"
            doc['livro'] = "P"
            list_temp.append(doc)
        # atualiza os n documentos da lista temporária
        collection.update(list_temp)
        # encerra contagem de tempo
        fim_couchdb = timeit.default_timer()
        # apresenta resultado
        print("========================================================================")
        print('ATUALIZANDO %i DOCUMENTO(S) NO COUCHDB' % n)
        print('DURACÃO: %f' % (fim_couchdb - inicio_couchdb))
        print("========================================================================")

    '''
        :func find_documents_couchdb
            - busca documentos no couchdb

        :parameters: 
            n - inteiro representado quantidade de documentos a serem buscados
    '''

    def find_documents_couchdb(self, n=0):
        # recupera collection do banco
        collection = self._conexao_couchdb['diplomas']
        # inicia a contagem de tempo
        inicio_couchdb = timeit.default_timer()
        resultado = ''
        # executa a busca n vezes
        for x in range(n):
            resultado = collection.find({
                "selector": {
                    "$or":
                        [
                            {'nivel_ensino': 'GRADUAÇÃO'},
                            {'nome_reitor': 'José Daniel Diniz Melo'},
                            {'livro': 'S'}

                        ]
                },
                "limit": 5000

            })
        # encerra a contagem de tempo
        fim_couchdb = timeit.default_timer()
        # apresenta resultado
        print("========================================================================")
        print('EXECUTANDO A BUSCA %i VEZ(ES) NO COUCHDB' % n)
        print('A CADA ITERAÇÃO FORAM RETORNADOS %i DOCUMENTOS' % len(list(resultado)))
        print('DURACÃO: %f' % (fim_couchdb - inicio_couchdb))
        print("========================================================================")

    '''
        :func find_documents_mongo
            - busca documentos no mongodb

        :parameters: 
            n - inteiro representado quantidade de documentos a serem buscados
    '''

    def find_documents_mongodb(self, n=0):
        # recupera base
        collection = self._conexao_mongo['diplomas']
        # inicia contagem de tempo
        inicio_mongo = timeit.default_timer()
        # executa a busca n vezes
        for x in range(n):
            resultado = collection.diplomas.find(
                {'$or': [
                    {'nivel_ensino': 'GRADUAÇÃO'},
                    {'nome_reitor': 'José Daniel Diniz Melo'},
                    {'livro': 'S'}],
                },
            ).limit(5000)
        # encerra contagem de tempo
        fim_mongo = timeit.default_timer()
        # apresenta resultado
        print("========================================================================")
        print('EXECUTANDO A BUSCA %i VEZ(ES) NO MONGODB' % n)
        print('BUSCANDO %i DOCUMENTOS' % len(list(resultado)))
        print('DURACÃO: %f' % (fim_mongo - inicio_mongo))
        print("========================================================================")


if __name__ == "__main__":

    path = '/home/wesleyadamo/Área de Trabalho/RESIDÊNCIA-LAIS/BD/diplomas-expedidos.json'
    abd = AnalisadorBD(path)
    # abre conexões com os bancos de dados
    abd.abrir_conexao_mongodb(host='localhost', port=27017)
    abd.abrir_conexao_couchdb(user='admin', password='root')

    # inserir documentos
    #abd.insert_documents_mongodb(1000)
    #abd.insert_documents_couchdb(1000)

    # buscar documentos
    # abd.find_documents_mongodb(1)
    # abd.find_documents_couchdb(1)

    #atualizar documentos
    #abd.update_documents_mongodb(1000)
    #abd.update_documents_couchdb(1000)

    # remover documentos
    #abd.remove_documents_mongodb(1)
    #abd.remove_documents_couchdb(1)

