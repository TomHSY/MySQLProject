#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Projet final de Gestion de Donnees - sujet 3
Juin 2021
Module de gestion simplifiee de la base de donnees du projet
@author: Lucille Caradec et Tom Hadrian Sy
"""

##############################################################################
### IMPORTATION DES PACKAGES 
##############################################################################
import pymysql
import csv
from prettytable import PrettyTable

##############################################################################
### FONCTIONS ET CLASSES
##############################################################################

class InvalidTableNameError(Exception):
    '''
    Classe d'exceptions : cas où un nom de table rentre ne correspond pas a la
    requete emise
    '''
    
    def __init__(self,value):
        message=f'''Le nom de la table saisi "{value}" ne
        correspond pas a la requete emise'''
        super().__init__(message)

class DataBase(object):
    '''
    Classe base de donnees
    '''
    
    #tentative de connection a la base de donnees Projet
    try:
        db = pymysql.connect(host = "localhost",
                             user = "root",
                             password = "",
                             db = "Projet",
                             port = 3306)
        cursor = db.cursor()
    
    #gestion des exceptions
    except Exception as e:
            print('Erreur lors de l\'ouverture de la base de donnee : ',e)
    
    def __init__(self):
        '''
        Constructeur de la classe : cree un objet de type DataBase
        '''
    
        self.tables = []    #liste des tables presentes dans la base de donnees
        self.importTables() #importation des tables deja existantes
                             
    def __str__(self):
        '''
        Affichage de la base de donnees
        ----------------------------------------------------------------------
        Returns :
                - un objet string decrivant la base de donnees
        ----------------------------------------------------------------------
        '''
        
        #en-tete
        intro = """
        -----------------------------------------------------------
        Base de donnees MySQL - Projet 
        -----------------------------------------------------------
        host = "localhost"
        user = "root"
        password = ""
        db = "Projet"
        port = 3306
        -----------------------------------------------------------\n
        """
        
        tables = 'Tables disponibles : \n\n'
        
        #cas où la base ne contient aucune table
        if self.tables == []:
            tables += 'Aucune pour l\'instant\n'
            
        #boucle sur les tables de la base et affichage
        else:
            for table in self.tables:
                tables += table.getName()   #importation du nom de la table
                str_table = '- '+table.__str__()+'\n\n'#impression de la table
                tables += str_table
                
        return intro+tables
    
    def close(self):
        '''
        Methode permettant de fermer la base de donnee'
        '''
        if self.db.open:
            self.db.close
            print('\nLa connexion a la base de donnees a ete fermee.')
            
    def importTables(self):
        '''
        Methode permettant d'importer des tables pre-existantes '
        '''
        
        #requete permettant d'obtenir le nom des tables presentes
        result = self.selectQuery('SHOW TABLES','fetchall')
        
        for res in result :
            #formatage du nom de la table 'Projet.(table)'
            name = f'Projet.{res[0]}'
            #creation d'instances de la classe Table
            self.tables.append(Table(name, name, import_=True))
    
    def createTable(self, query, newTableName):
        '''
        Methode permettant a l'utilisateur de creer une nouvelle table
        ----------------------------------------------------------------------
        Arguments:
                - query : la requete a emettre de type string
                - newTableName : le nom de la nouvelle table a creer
        ----------------------------------------------------------------------
        Returns :
                - un objet de type Table, la nouvelle table
        ----------------------------------------------------------------------
        '''
        #constitution de la liste des noms de tables (existantes)
        L_tableNames = [table.getName() for table in self.tables]
        
        #si le nom de la nouvelle table n'est pas present dans la liste
        #(non deja presente dans la base)
        if newTableName not in L_tableNames:
            #creation d'une instance de la classe Table
            table = Table(query, newTableName)
            self.tables.append(table)   # ajout a la liste des tables
            
        #si le nom rentre correspond a une table deja existante
        else:
            print(f'La table {newTableName} existe deja.')
            #importation d'une instance de la classe Table depuis la liste
            table = self.tables[L_tableNames.index(newTableName)]
        
        #renvoie de l'objet Table
        return table
               
    def commitQuery(self,query,content=None):
        '''
        Methode permettant a l'utilisateur d'emettre une requete avec 'commit'
        a la base de donnees, tout en gerant les potentielles exceptions
        ----------------------------------------------------------------------
        Arguments :
                - query : la requete a emettre de type string
                - content : argument facultatif representant un contenu a
                    associer a la requete (requetes de type INSERT)
        ----------------------------------------------------------------------
        '''
        #tentative de requete
        try:
            
            #si un contenu doit etre associe a la requete
            if content:
                #emission de la requete par le curseur
                self.cursor.execute(query,content)
            #cas inverse
            else:
                #emission de la requete par le curseur
                self.cursor.execute(query)
                
            self.db.commit()
        
        #gestion des exceptions
        except self.db.Error as e:
            print('Erreur : ',e)
               
    def selectQuery(self,query,fetching='fetchall'):
        '''
        Methode permettant a l'utilisateur d'emettre une requete de type 
        'select' a la base de donnees, tout en gerant les potentielles 
        exceptions associees
        ----------------------------------------------------------------------
        Arguments :
                - query : la requete a emettre de type string
                - fetching : un argument facultatif de type string indiquant 
                si la requete met en jeu un fetchone ou fetchall 
                (option par defaut)
        ----------------------------------------------------------------------
        Returns :
                - les donnees corespondantes a la requete emise (SELECT)
        '''
        
        #tentative d'emission de la requete
        try:
            self.cursor.execute(query)
            # emission de la requete selon la valeur de l'argument fetching
            data = eval('self.cursor.'+fetching+'()')
            #renvoie des donnees
            return data
        
        #gestion des exceptions
        except self.db.Error as e:
            print('Erreur : ',e)
            #renvoie d'un None
            return None
                    
    
class Table(DataBase):
    '''
    Classe Table
    '''
    
    def __init__(self,query,tableName,import_=False):
        '''
        Constructeur de la classe : cree un objet de type Table
        ----------------------------------------------------------------------
        Arguments :
                - query : la requete CREATE TABLE a emettre de type string
                - tableName : nom de la table a creer (string)
                - import_ : argument facultatif explicitant si la table 
                doit etre creee (defaut) ou simplement importee parmi 
                les tables deja existantes
        ----------------------------------------------------------------------
        '''
    
        # tentative d'emission de la requete
        try:
            
            #gestion du cas où le nom de la table rentre ne correspondrait pas
            #a celui indique dans la requete
            if tableName not in query:
                
                #levee de l'exception InvalidTableNameError
                raise InvalidTableNameError(tableName)
                
            #cas où les noms de table se correspondent
            else:
                
                #declaration du nom de la table comme variable de classe
                self.tableName = tableName
                
                #si l'option import_ vaut True, la requete CREATE TABLE, n'est
                #pas emise, car une table existe deja dans la base
                #Cependant l'objet Table doit etre cree dans l'environnement 
                #Python, ce qui equivaut a une simple importation
                if not import_:
                    self.commitQuery(query)
                    
        #gestion des exceptions
        except Exception as e:
            print('Erreur : ',e)
            
                    
    def __str__(self):
        '''
        Affichage de la table
        ----------------------------------------------------------------------
        Returns :
                - un objet string decrivant la table
        ----------------------------------------------------------------------
        '''
        
        #tentative de requete
        try:
            
            #requete permettant d'obtenir toutes les donnees de la table
            query = f'select * from {self.tableName}'
            data = self.selectQuery(query)
            
            #requete permettant d'obtenir les informations de la table
            #(nom et type des attributs)
            query = f'describe {self.tableName}'
            table_info = self.selectQuery(query)
            
            #initialisation d'un objet PrettyTable
            output = PrettyTable()
            
            #constitution de l'en-tete du PrettyTable
            L_colnames = [f'{info[0]} ({info[1]})' for info in table_info]
            output.field_names = L_colnames
            
            #boucle sur les premieres lignes des donnees de la table
            for elem in data[:5]:
                #ajout de lignes du PrettyTable
                output.add_row([elem[x] for x in range(len(elem))])
                
            output.add_row(['...']*len(elem))
        
        #gestion des exceptions
        except:
            #si la requete echoue, il s'agit sûrement du fait que la table est
            #vide
            output = f'La table {self.tableName} est vide.'
        
        #renvoie du texte a afficher
        return str(output)
    
    def getName(self):
        '''
        Methode retournant le nom de la table
        ----------------------------------------------------------------------
        Returns :
                - self.tableName : le nom de la table (string)
        ----------------------------------------------------------------------
        '''
        return self.tableName
     
    def drop(self):
        '''
        Methode permettant de supprimer la table, au besoin
        ''' 
        
        #expression de la requete de suppression
        query = f"""drop table {self.tableName}"""
        
        #tentative d'emission de la requete
        try:
            self.commitQuery(query)
            
            #obtention de l'indice de la table a supprimer, a partir de son nom
            #et suppression de la table
            tableNames = [table.getName() for table in self.tables]
            del(self.tables[tableNames.index(self.tableName)])
            print(f"La table {self.tableName} a ete supprimee de la base de donnees.")
        
        #gestion des exceptions
        except Exception as e:
            print(e)
            print(f"La table {self.tableName} n\'a pas pu etre supprimee.")
            
    def getRowNumber(self, filename):
        '''
        Methode permettant d'obtenir le nombre de lignes total d'un csv
        ----------------------------------------------------------------------
        Arguments :
                - filename : le nom du fichier (string)
        ----------------------------------------------------------------------
        Returns :
                - le nombre de lignes (int)
        ----------------------------------------------------------------------
        '''
        with open(filename, 
                  newline = '\n', 
                  encoding='utf-8-sig') as csvfile:
            return len(csvfile.readlines())
    
    def replaceTable(self):
        '''
        Methode de gerer le cas où l'utilisateur veut remplir une table qui
        possede deja un contenu
        ----------------------------------------------------------------------
        Returns :
                - un booleen indiquant si les donnees peuvent etre ecrasees
        ----------------------------------------------------------------------
        '''
        
        #requete permettant le test d'existence d'un contenu dans la table
        res = self.selectQuery(f'select * from {self.tableName}')
        rep = 'o'
        
        #si la table n'est pas vide, l'utilisateur est questionne
        if res != ():
            
            #boucle while gerant le cas où l'utilisateur donne une mauvaise
            #reponse
            continue_ = True
            while continue_:
                rep = input(f'> La table {self.tableName} n\'est pas vide.'
                            ' Voulez-vous remplacer son contenu (o / n) ?\n'
                            '    >')
                if rep in ['o','n']:
                    #sortie de la boucle while
                    continue_ = False
                    
        #renvoie de la decision de l'utilisateur sous forme de booleen
        return rep == 'o'

    def fill(self, query, filename, conditions=None):
        '''
        Methode remplissant une table avec les donnees du csv correspondant
        ----------------------------------------------------------------------
        Arguments :
                - query : la requete a emettre, de type string
                - filename : le nom du fichier (string)
                - conditions : argument facultatif contenant d'eventuelles
                    conditions sur le remplissage de la table
                    (expression de type string)
        ----------------------------------------------------------------------
        '''
        
        #test de l'existence prealable d'un contenu de la table a remplir et
        #reception de l'action donnee par l'utilisateur (conserver ou ecraser)
        replace = self.replaceTable()
        
        #cas où les donnees pre-existantes sont conservees
        if not replace:
            print(f"La table {self.tableName} n'a pas ete modifiee.")
        
        #cas où les donnees sont ecrasees
        else:
            
            #obtention du nombre de ligne total du fichier csv
            nrow = self.getRowNumber(filename)
            
            # ouverture du fichier csv source
            with open(filename, 
                      newline = '\n', 
                      encoding='utf-8-sig') as csvfile:
                data=csv.reader(csvfile, delimiter=';')
                
                #defilement d'une ligne (passage de l'en-tete du fichier)
                next(data)
                
                #boucle sur les lignes du fichier
                for i,row in enumerate(data):
                    print(f'\rLoading data... - {round((i/nrow)*100,2)}%'
                          ' completed -', end='',flush=True)
    
                    #si aucune condition n'est specifie, la variable prend
                    #la valeur True
                    if not conditions:
                        conditions = 'True'
                        
                    #evaluation des conditions de remplissage pour chaque ligne
                    if eval(conditions):
                        #execution de la requete de remplissage
                        self.commitQuery(query,row)
    
            print(f"La table {self.tableName} a bien ete remplie.")