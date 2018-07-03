#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sys
from hs_utils import imdb_handler

list_terms = '/home/renato/workspace/projects/home_services/src/imdb_collector/config/'
args = {'list_terms':   list_terms}
ih = imdb_handler.IMDbHandler(**args)

def look_imdb_data(title, do_test=True, show_debug=False):
    if do_test:
        clean_name           = ih.clean_sentence(title, debug=show_debug)
        year_found, splitted = ih.skim_title(clean_name)
        items                = ih.get_imdb_best_title(splitted, year_found=year_found)
        print "===> torrent_title:\t ", title
        print "===> clean_name:\t ", clean_name
        print "===> torrent_query:\t ", splitted
        print "===> torrent_year:\t ", year_found
        
        # print "---> items:", items
        if len(items)>0:
            items_keys  = items.keys()
            items_size  = len(items['imdb_info']) if 'imdb_info' in items_keys else 0 
            if items_size>0 and 'imdb_info' in items_keys:
                imdb_info   = items['imdb_info']
                for item_info in imdb_info:
                    imdb_info_k = item_info.keys()
                    if 'score' in imdb_info_k:
                        print "===> imdb_score:\t ", item_info['score']
                    if 'title' in imdb_info_k:
                        print "===> imdb_title:\t ", item_info['title']
                    if 'year' in imdb_info_k:
                        print "===> imdb_year:\t\t ", item_info['year']
                    print ""
            else:
                print "===> NO IMDB DATA"
                print ""
        else:
            print "===> NO DATA"
            print ""

do_only_if = 0
do_test = 0
title = 'Ted 2O12'
look_imdb_data(title, do_test=do_test)

title = 'This Is The End 2013 HDTS XviD CHEESE'
look_imdb_data(title, do_test=do_test)

title = "The Great Gatsby 2013 ENG DVDRIP XviD ENABLE"
look_imdb_data(title, do_test=do_test)

title = "A Good Day To Die Hard GreatQual RLqll"
look_imdb_data(title, do_test=do_test)

title = "Abus de Faiblesse 1080p BRRip x264 AAC-RARBG 2014"
look_imdb_data(title, do_test=do_only_if)

title = "The Marriage Counselor 720p Bluray x264-TDM"
look_imdb_data(title, do_test=do_test)

title = "Rosaline [1080p] 2014"
look_imdb_data(title, do_test=do_test)

title = "St Vincent de Van Nuys [Eng]BlueLady"
look_imdb_data(title, do_test=do_test)

title = "Frankie & Alice DVDRip[Xvid]AC3 6ch[Eng]BlueLady"
look_imdb_data(title, do_test=do_test)

title = "Dawn DVDRip NL Subs DutchReleaseTeam 2014"
look_imdb_data(title, do_test=do_test)

title = "LEGO XviD-ECSTASY"
look_imdb_data(title, do_test=do_test)

title = "Kitchen Sink XviD-ECSTASY"
look_imdb_data(title, do_test=do_test)

title = "Grace of Monaco XviDMovieTorrentz MovieTorrentz"
look_imdb_data(title, do_test=do_test)

title = "Argo DvDrip AC3[Eng]-aXXo 2014"
look_imdb_data(title, do_test=do_test)

title = "Promised Land XviDECSTASY"
look_imdb_data(title, do_test=do_test)

title = "The End of the World DvDrip[Eng]-FXG"
look_imdb_data(title, do_test=do_test)

title = "Raaz 3 DvDrip[FR-SUB]-NikonXp"
look_imdb_data(title, do_test=do_test)

title = "Taken 2 XviD-ECSTASY"
look_imdb_data(title, do_test=do_test)

title = "Percy Jackson & the Olympians The Sea of Monsters 1080p WEB-DL DD5 1 H 264-ECI"
look_imdb_data(title, do_test=do_test)

title = "Percy Jackson the Olympians The Sea of Monsters CAM XViD IMAGiNE 2014"
look_imdb_data(title, do_test=do_test)

title = "The Penguins of Madagascar TS LiNE [H264 MP4][RoB]"
look_imdb_data(title, do_test=do_test)

title = "Could Atlas 2012 BRRip READNFO XviD BiDA"
look_imdb_data(title, do_test=do_only_if)

title = "The Boy with the Cuckoo-Clock Heart REPACK DVDRip 2014"
look_imdb_data(title, do_test=do_only_if)

title = "The Bitter Pill DVDRiP XviD Movie Torrentz Movie Torrentz"
look_imdb_data(title, do_test=do_only_if)

title = "Les Bandits manchots [DVDRip XviD]"
look_imdb_data(title, do_test=do_only_if)

title = "Le petit joueur DVDRip JayBob HQ"
look_imdb_data(title, do_test=do_only_if)

title = "Les Bandits manchots DVDRip[Xvid]AC3 6ch[Eng]BlueLady"
look_imdb_data(title, do_test=do_only_if)

title = "The World s End 2013 ENG DVDRip XviD SAiMORNY"
look_imdb_data(title, do_test=do_only_if)

title = "The Apocalypse 2012 HD EXTENDED 1080p 2014"
look_imdb_data(title, do_test=do_only_if)

title = "300 Battle of Artemisia BDRip XviD COCAIN"
look_imdb_data(title, do_test=do_only_if)

title = "A Glimpse Inside the Mind of Charles Swan III (2012) DVDRip[Xvid AC3[5 1]-RoCK&B.."
look_imdb_data(title, do_test=do_test)

title = "A Glimpse Inside the Mind of Charles Swan III (2012) DvDrip[FR-SUB]-NikonXp"
look_imdb_data(title, do_test=do_test)

title = "Django Unchained iTALiAN MD TELESYNC V2 XviD-REV[MT]"
look_imdb_data(title, do_test=do_test)

title = "White House Down DVDRip XviD AXXP"
look_imdb_data(title, do_test=do_test)

title = "House at the End of the Street DVDRip XviD AXXP"
look_imdb_data(title, do_test=do_test)

title = "Как приручить дракона 2   How to Train Your Dragon 2 (2014) W.."
look_imdb_data(title, do_test=do_test)

title = "All Is Lost (2013) DVDRip rar"
look_imdb_data(title, do_test=do_only_if)

title = "Days Of Thunder (1990) 720p BR Rip x264 [ HINDI ] -« Im Loser -«"
look_imdb_data(title, do_test=do_test, show_debug=False)
