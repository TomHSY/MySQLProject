#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Projet final de Gestion de Donnees - sujet 3
Juin 2021
Fichier principal : construction de la base de donnees et analyses
@author: Lucille Caradec et Tom Hadrian Sy
"""

##############################################################################
### IMPORTATION DES PACKAGES 
##############################################################################
import MySQLManager
from sklearn.linear_model import LinearRegression
import numpy
from matplotlib import pyplot as plt

##############################################################################
### PROGRAMME PRINCIPAL
##############################################################################

try: #tentatives de requetes
    
    #######################################################
    ### 1. Importation des tables necessaires aux analyses
    #######################################################
    
    #Importation de la base de donnees
    BDD = MySQLManager.DataBase()
    
    ### Table BAC
    #Creation de la table BAC
    requete1 = '''CREATE TABLE Projet.bac (id INT PRIMARY KEY AUTO_INCREMENT,
                                    etablissement VARCHAR(150) NOT NULL ,  
                                    annee INT NOT NULL ,  
                                    ville VARCHAR(50) NOT NULL ,  
                                    academie VARCHAR(50) NOT NULL ,  
                                    departement VARCHAR(50) NOT NULL ,  
                                    taux_brut INT NOT NULL ,  
                                    taux_france INT NOT NULL)ENGINE = InnoDB;'''
    Table_Bac = BDD.createTable(query = requete1, newTableName='Projet.bac')
    BDD.commitQuery('ALTER TABLE Projet.bac ADD INDEX nom_complet')
    print(Table_Bac)
    input('Appuyez sur Enter pour continuer')
    
    #Remplissage de la table BAC
    requete2 = """insert into Projet.bac (etablissement, annee, ville, 
            academie, departement,taux_brut, taux_france)
            values (%s, %s, %s, %s, %s, %s, %s)"""
    Table_Bac.fill(requete2,'resultats_bac.csv',
                        conditions=
                        "row[-1]!='ND' and row[-2]!='ND' and (row[1]=='2019'"+
                        "or row[1]=='2018')")
            #on selectionne uniquement les lignes avec des taux de resussite 
            #non vides et pour les annees 2018 et 2019
    print(Table_Bac)
    input('Appuyez sur Enter pour continuer')

    ### Table DPT
    #Creation de la table Dpt
    requete3 = """CREATE TABLE Projet.dpt (departement VARCHAR(50) NOT NULL ,
                                        region VARCHAR(50) NOT NULL,
                                        PRIMARY KEY (departement))
                    ENGINE = InnoDB;"""
    
    Table_Dpt = BDD.createTable(query = requete3, newTableName='Projet.dpt')
    print(Table_Dpt)
    input('Appuyez sur Enter pour continuer')
    
    #Remplissage de la table Dpt
    requete4 = """ INSERT INTO Projet.dpt ( departement, region )
                    VALUES (%s, %s)
               """
    Table_Dpt.fill(requete4, 'dpt_regions.csv')
    print(Table_Dpt)
    input('Appuyez sur Enter pour continuer')

    ### Table Climat
    #Creation de la table Climat
    requete5 = """CREATE TABLE Projet.climat ( id INT PRIMARY KEY AUTO_INCREMENT,
                                    region VARCHAR(50) NOT NULL,
                                    jour DATE NOT NULL,
                                    TempMax FLOAT NOT NULL,
                                    TempMin FLOAT NOT NULL,
                                    Vent FLOAT NOT NULL,
                                    Humidite FLOAT NOT NULL,
                                    Visibilite FLOAT NOT NULL,
                                    Couverture_Nuage FLOAT NOT NULL) 
            ENGINE = InnoDB;
        """
    Table_Clim = BDD.createTable(query = requete5, newTableName='Projet.climat')
    print(Table_Clim)
    input('Appuyez sur Enter pour continuer')

    #Remplissage de la table Climat
    requete6 = """ insert into Projet.climat (region, jour, TempMax, TempMin, 
                    Vent, Humidite,Visibilite, Couverture_Nuage)
                    values (%s, %s, %s, %s, %s, %s, %s, %s)
                """
    Table_Clim.fill(requete6,'climat.csv')
    print(Table_Clim)
    input('Appuyez sur Enter pour continuer')
    
    ### Affichage de la base de donnees
    print(BDD)

except Exception as e:
    print('Exception detectee : ', e)


try :
    #########################################################################
    ### 2. Requetes croisant les differentes tables, pour obtenir les donnees
    #########################################################################
    print("##################################################################")
    print("""Problematique generale :
    
    On cherche a savoir si le climat influence les resultats du bac 
    sur une annee donnee. En effet, on pourrait imaginer qu'un 
    tres beau temps n'encourage pas les eleves a travailler!
    
    Pour repondre a cette question, on va etudier les taux de 
    reussite au bac selon les temperatures moyennes par region 
    sur une annee (axe 1) / les temperatures moyennes pour une 
    region, sur des annees differentes (axe 2).""")
    print("##################################################################")
    
    print("\n----------------------------------------------------------------\n")
    print("Axe 1 : on compare les resultats au bac sur l'annee 2019 selon "
          "la temperature moyenne des regions de France")
    
    periode = [('avril', '4'), ('mai', '5'), ('juin', '6'), ('juillet','7')]
    
    #on cree une liste pour contenir les resultats de la regression par mois
    stats = []
    
    for mois in periode:
        Q1 = f""" SELECT b.taux_brut, AVG(c.TempMax) TempMax, 
                AVG(c.TempMin) TempMin FROM Bac b
                INNER JOIN Dpt d ON b.departement = d.departement
                INNER JOIN Climat c ON c.region = d.region
                WHERE b.annee = 2019 AND YEAR(c.jour) = 2019 
                AND MONTH(c.jour) = {mois[1]} GROUP BY b.id"""
    
        res = BDD.selectQuery(Q1,'fetchall')
        
        # mise en forme des donnees pour la creation d'un graphe matplotlib
        x = []
        y = []
        for elem in res :
            x.append((elem[1]+elem[2])/2)   
            # temperature moyenne = moyenne du min et max journalier
            y.append(elem[0])
        x = numpy.array(x).reshape(-1,1)
        y = numpy.array(y).reshape(-1,1)
        
        #calcul d'une droite de regression
        modele = LinearRegression()
        modele.fit(x,y)
        
        #calcul des indicateurs : pente, intercept, R2
        tmp = []
        tmp.append(modele.intercept_)
        tmp.append(modele.coef_)
        tmp.append(modele.score(x,y))
        
        stats.append(tmp)
        
        #creation du graphe
        plt.plot(x, y, 'bo' , alpha=0.1)
        
        xplot = x.tolist()
        yplot = modele.predict(xplot)
        
        # plot de la droite de regression
        plt.plot(xplot, yplot, 'r-')
        
        plt.title("Taux de reussite au baccalaureat en fonction de la" 
                  f"temperature en {mois[0]}")
        plt.xlabel(f"Temperature moyenne par region en {mois[0]}")
        plt.ylabel("Taux de reussite au baccalaureat en 2019")
        plt.show()
        
        print(f"\nMois : {mois[0]}")
        print(f">>  Scores de la regression lineaire : \nIntercept {tmp[0]} "
              f"|| Slope : {tmp[1]}")
        print(f"R2 : {tmp[2]}")
        
    print("\nIntepretation : \nCette etude nous a montre que globalement, les " 
          "resultats du bac sont tres peu lies a la temperature. En effet, on"
          " a des R2 tres faibles lorsque l'on cherche une relation lineaire "
          "entre resultats au bac et temperature moyenne en 2019, quelque soit"
          " le mois de l'annee. On suppose qu'il existe, pour une region "
          "donnee (et une temperature moyenne donnee), plusieurs etablissements "
          "possedant des resultats tres heterogenes.")
    
    """
    Axe 2 : on regarde les resultats au bac selon les temperatures par annee
    dans la ville de Lille, sur les mois de mai et juin
    """
    
    print("\n-----------------------------------------------\n")
    print("Partie 2 : on compare les resultats au bac selon les temperatures "+
          "par annee dans la ville de Lille, sur les mois de mai et juin\n")
    
    Q2 = """SELECT AVG((TempMax + TempMin)/2) TempMoy, YEAR(jour)
              FROM Climat
              WHERE MONTH(jour) IN (5,6) AND region = "HAUTS-DE-FRANCE"
              GROUP BY YEAR(jour)"""
              
    res = BDD.selectQuery(Q2, 'fetchall')
    
    print("Temperatures moyennes a Lille en mai et juin :")
    
    for elem in res :
        print(f"{elem[1]} : {elem[0]}")
    
    print("On voit que dans la region des Hauts-De-France, la temperature "+
          "moyenne a diminue entre 2018 et 2019. On va regarder les effets sur "+
          "les etablissements.")
    
    Q3 = """SELECT b.taux_brut taux_2018, bb.taux_brut taux_2019
              FROM Bac b
              INNER JOIN Bac bb
              ON b.etablissement = bb.etablissement
              WHERE b.ville = 'LILLE' AND b.annee = 2018 AND bb.annee = 2019"""
              
    res = BDD.selectQuery(Q3, 'fetchall')
    aug = 0
    dim = 0
    meme = 0
    
    for elem in res :
        if elem[1] > elem[0] :
            aug+=1
        elif elem[1] < elem[0] :
            dim +=1
        else :
            meme +=1
    
    print("\nComparaison des etablissements Lillois entre 2018 et 2019")
    print(f"Augmentation : {aug} || Diminution : {dim} || Meme taux : {meme}")
        
    
#gestion des exceptions 
except Exception as e:
    print('Exception detectee : ',e)

#fermeture finale de la base de donnees
finally:
    try :
        BDD.close()
    except:
        pass
        
        



