import sqlite3
import random
from datetime import datetime, timedelta
import uuid

product_vendors = {
    # Apparel
    "T-Shirts": ['Nike', 'Adidas', 'Puma', 'H&M', 'Zara', 'Uniqlo', 'Gap', 'Forever 21', 'American Eagle', 'Hollister', 'Levi\'s', 'Tommy Hilfiger', 'Calvin Klein', 'Ralph Lauren', 'Under Armour', 'Champion', 'Gucci', 'Versace', 'Dolce & Gabbana', 'Balenciaga'],
    "Shirts": ['Zara', 'H&M', 'Uniqlo', 'Gap', 'Tommy Hilfiger', 'Ralph Lauren', 'Calvin Klein', 'Hugo Boss', 'Brooks Brothers', 'J.Crew', 'Banana Republic', 'Armani', 'Dolce & Gabbana', 'Gucci', 'Versace', 'Burberry', 'Ted Baker', 'COS', 'Acne Studios'],
    "Blouses": ['Zara', 'H&M', 'Mango', 'Forever 21', 'Topshop', 'Ann Taylor', 'Loft', 'Theory', 'Diane von Furstenberg', 'Kate Spade', 'Tory Burch', 'Calvin Klein', 'Ralph Lauren', 'Armani', 'Gucci', 'Prada', 'Chanel', 'Valentino'],
    "Tunics": ['H&M', 'Zara', 'Mango', 'Forever 21', 'Anthropologie', 'Free People', 'Eileen Fisher', 'J.Jill', 'Chico\'s', 'Coldwater Creek', 'Talbots', 'White House Black Market'],
    "Sweaters": ['Uniqlo', 'Gap', 'J.Crew', 'Banana Republic', 'Ralph Lauren', 'Tommy Hilfiger', 'Calvin Klein', 'H&M', 'Zara', 'Mango', 'COS', 'Acne Studios', 'Theory', 'Everlane', 'Madewell', 'Armani', 'Gucci', 'Prada', 'Burberry', 'Saint Laurent'],
    "Hoodies": ['Nike', 'Adidas', 'Puma', 'Champion', 'Under Armour', 'The North Face', 'Gap', 'H&M', 'Zara', 'American Eagle', 'Hollister', 'Forever 21', 'Tommy Hilfiger', 'Calvin Klein', 'Ralph Lauren', 'Gucci', 'Balenciaga', 'Versace', 'Off-White', 'Supreme'],
    "Jackets": ['The North Face', 'Patagonia', 'Columbia', 'Nike', 'Adidas', 'Puma', 'Zara', 'H&M', 'Mango', 'Topshop', 'Calvin Klein', 'Tommy Hilfiger', 'Ralph Lauren', 'Burberry', 'Gucci', 'Prada', 'Balenciaga', 'Moncler', 'Canada Goose', 'Arc\'teryx'],
    "Coats": ['Zara', 'H&M', 'Mango', 'Topshop', 'Burberry', 'Max Mara', 'Calvin Klein', 'Ralph Lauren', 'Tommy Hilfiger', 'Michael Kors', 'Coach', 'Kate Spade', 'Gucci', 'Prada', 'Balenciaga', 'Moncler', 'Canada Goose', 'Woolrich', 'Mackage'],
    "Blazers": ['Zara', 'H&M', 'Mango', 'Topshop', 'J.Crew', 'Banana Republic', 'Brooks Brothers', 'Calvin Klein', 'Ralph Lauren', 'Tommy Hilfiger', 'Hugo Boss', 'Armani', 'Gucci', 'Prada', 'Saint Laurent', 'Balenciaga', 'Theory', 'COS'],
    "Suits": ['Hugo Boss', 'Armani', 'Calvin Klein', 'Ralph Lauren', 'Tommy Hilfiger', 'Brooks Brothers', 'J.Crew', 'Banana Republic', 'Zara', 'H&M', 'Mango', 'Suitsupply', 'Paul Smith', 'Gucci', 'Prada', 'Dolce & Gabbana', 'Saint Laurent', 'Balenciaga', 'Versace'],
    "Jeans": ['Levi\'s', 'Wrangler', 'Lee', 'Gap', 'H&M', 'Zara', 'Uniqlo', 'American Eagle', 'Hollister', 'Abercrombie & Fitch', 'Calvin Klein', 'Tommy Hilfiger', 'Ralph Lauren', 'Diesel', 'True Religion', '7 For All Mankind', 'Citizens of Humanity', 'AG Jeans', 'Paige', 'Frame'],
    "Trousers": ['Zara', 'H&M', 'Uniqlo', 'Gap', 'J.Crew', 'Banana Republic', 'Brooks Brothers', 'Calvin Klein', 'Ralph Lauren', 'Tommy Hilfiger', 'Hugo Boss', 'Armani', 'Gucci', 'Prada', 'Theory', 'COS', 'Everlane', 'Madewell'],
    "Pants": ['Zara', 'H&M', 'Uniqlo', 'Gap', 'American Eagle', 'Hollister', 'Abercrombie & Fitch', 'Levi\'s', 'Calvin Klein', 'Tommy Hilfiger', 'Ralph Lauren', 'Nike', 'Adidas', 'Puma', 'Under Armour', 'Lululemon', 'Athleta', 'Alo Yoga'],
    "Shorts": ['Nike', 'Adidas', 'Puma', 'Under Armour', 'H&M', 'Zara', 'Uniqlo', 'Gap', 'American Eagle', 'Hollister', 'Abercrombie & Fitch', 'Levi\'s', 'Calvin Klein', 'Tommy Hilfiger', 'Ralph Lauren', 'Patagonia', 'The North Face', 'Columbia'],
    "Skirts": ['Zara', 'H&M', 'Mango', 'Forever 21', 'Topshop', 'Ann Taylor', 'Loft', 'J.Crew', 'Banana Republic', 'Calvin Klein', 'Ralph Lauren', 'Tommy Hilfiger', 'Kate Spade', 'Tory Burch', 'Diane von Furstenberg', 'Gucci', 'Prada', 'Chanel', 'Valentino'],
    "Dresses": ['Zara', 'H&M', 'Mango', 'Forever 21', 'Topshop', 'ASOS', 'Reformation', 'Free People', 'Anthropologie', 'Ann Taylor', 'Loft', 'J.Crew', 'Banana Republic', 'Calvin Klein', 'Ralph Lauren', 'Diane von Furstenberg', 'Kate Spade', 'Tory Burch', 'Gucci', 'Prada', 'Chanel', 'Valentino'],
    "Jumpsuits": ['Zara', 'H&M', 'Mango', 'Forever 21', 'Topshop', 'ASOS', 'Reformation', 'Free People', 'Anthropologie', 'Ann Taylor', 'Loft', 'J.Crew', 'Banana Republic', 'Calvin Klein', 'Ralph Lauren', 'Diane von Furstenberg', 'Kate Spade', 'Tory Burch'],
    "Leggings": ['Lululemon', 'Athleta', 'Alo Yoga', 'Nike', 'Adidas', 'Puma', 'Under Armour', 'Zella', 'Gap', 'H&M', 'Zara', 'Uniqlo', 'American Eagle', 'Fabletics', 'Outdoor Voices', 'Sweaty Betty'],
    "Tracksuits": ['Nike', 'Adidas', 'Puma', 'Under Armour', 'Reebok', 'New Balance', 'Fila', 'Champion', 'Tommy Hilfiger', 'Calvin Klein', 'Ralph Lauren', 'Gucci', 'Balenciaga', 'Versace', 'Fendi', 'Juicy Couture'],
    "Overalls": ['Levi\'s', 'Carhartt', 'Dickies', 'Gap', 'H&M', 'Zara', 'Uniqlo', 'American Eagle', 'Hollister', 'Abercrombie & Fitch', 'Free People', 'Urban Outfitters', 'Madewell', 'Forever 21'],
    "Sarees": ['Fabindia', 'Biba', 'Global Desi', 'Anita Dongre', 'Sabyasachi', 'Manish Malhotra', 'Tarun Tahiliani', 'Rohit Bal', 'Ritu Kumar', 'W for Woman', 'Libas', 'Janasya', 'Varanga', 'Sangria', 'Ethnix', 'Aurelia'],
    "Kurtas": ['Fabindia', 'Biba', 'Global Desi', 'Anita Dongre', 'W for Woman', 'Libas', 'Janasya', 'Varanga', 'Sangria', 'Manyavar', 'Ethnix', 'Aurelia', 'Rangmanch', 'Good Earth'],
    "Lehengas": ['Sabyasachi', 'Manish Malhotra', 'Tarun Tahiliani', 'Rohit Bal', 'Anita Dongre', 'Ritu Kumar', 'Biba', 'Global Desi', 'W for Woman', 'Libas', 'Janasya', 'Varanga', 'Sangria'],
    "Abayas": ['Saudi Designers', 'D\'NA', 'Hayaat Collection', 'Sharaarah', 'Shouq', 'Shadya', 'Modanisa', 'Abaya Addict', 'Abaya Boulevard', 'Hayah Collection'],
    "Cardigans": ['Uniqlo', 'Gap', 'J.Crew', 'Banana Republic', 'Ralph Lauren', 'Tommy Hilfiger', 'Calvin Klein', 'H&M', 'Zara', 'Mango', 'COS', 'Acne Studios', 'Theory', 'Everlane', 'Madewell', 'Armani', 'Gucci', 'Prada', 'Burberry', 'Saint Laurent'],
    "Tank Tops": ['H&M', 'Zara', 'Uniqlo', 'Gap', 'American Eagle', 'Hollister', 'Abercrombie & Fitch', 'Nike', 'Adidas', 'Puma', 'Under Armour', 'Calvin Klein', 'Tommy Hilfiger', 'Ralph Lauren', 'Lululemon', 'Athleta', 'Alo Yoga'],
    "Vests": ['The North Face', 'Patagonia', 'Columbia', 'Nike', 'Adidas', 'Puma', 'Uniqlo', 'Gap', 'H&M', 'Zara', 'Calvin Klein', 'Tommy Hilfiger', 'Ralph Lauren', 'Arc\'teryx', 'Marmot', 'Mountain Hardwear'],
    "Shawls": ['Fabindia', 'Biba', 'Global Desi', 'Anita Dongre', 'W for Woman', 'Ritu Kumar', 'Good Earth', 'Ethnix', 'Aurelia', 'Rangmanch', 'Pashmina Boutique', 'Kashmir Loom'],
    "Scarves": ['Burberry', 'Gucci', 'Hermès', 'Louis Vuitton', 'Prada', 'Dolce & Gabbana', 'Fendi', 'Versace', 'Zara', 'H&M', 'Mango', 'Uniqlo', 'Gap', 'Banana Republic', 'J.Crew'],
    "Dupattas": ['Fabindia', 'Biba', 'Global Desi', 'Anita Dongre', 'W for Woman', 'Ritu Kumar', 'Good Earth', 'Ethnix', 'Aurelia', 'Rangmanch', 'Libas', 'Janasya'],
    "Underwear": ['Calvin Klein Underwear', 'Hanes', 'Fruit of the Loom', 'Jockey', 'Tommy Hilfiger', 'Ralph Lauren', 'H&M', 'Uniqlo', 'Gap', 'Victoria\'s Secret', 'PINK', 'Savage X Fenty', 'ThirdLove', 'Warners', 'Maidenform'],
    "Bras": ['Victoria\'s Secret', 'Calvin Klein Underwear', 'ThirdLove', 'Savage X Fenty', 'Wacoal', 'Natori', 'Chantelle', 'Panache', 'Freya', 'Elomi', 'Lively', 'True & Co', 'Aerie', 'PINK', 'Maidenform', 'Warners'],
    "Panties": ['Victoria\'s Secret', 'Calvin Klein Underwear', 'Aerie', 'PINK', 'Savage X Fenty', 'Hanky Panky', 'Commando', 'Cosabella', 'Fleur du Mal', 'Journelle', 'ThirdLove', 'H&M', 'Uniqlo', 'Gap', 'Jockey', 'Maidenform'],
    "Boxers": ['Calvin Klein Underwear', 'Tommy Hilfiger', 'Ralph Lauren', 'Hanes', 'Fruit of the Loom', 'Jockey', 'Saxx', '2(x)ist', 'Andrew Christian', 'MeUndies', 'Pair of Thieves', 'H&M', 'Uniqlo', 'Gap'],
    "Lingerie Sets": ['Victoria\'s Secret', 'Calvin Klein Underwear', 'La Perla', 'Agent Provocateur', 'Savage X Fenty', 'ThirdLove', 'Hanky Panky', 'Journelle', 'Fleur du Mal', 'Cosabella', 'Natori', 'Chantelle', 'Wacoal'],
    "Sleepwear": ['Victoria\'s Secret', 'PINK', 'Aerie', 'Calvin Klein Underwear', 'Soma', 'Natori', 'Eberjey', 'PajamaGram', 'Sleepy Jones', 'Gap', 'H&M', 'Uniqlo', 'J.Crew', 'Banana Republic', 'Nordstrom'],
    "Nightgowns": ['Victoria\'s Secret', 'PINK', 'Aerie', 'Soma', 'Natori', 'Eberjey', 'Journelle', 'Nordstrom', 'Bloomingdale\'s', 'Macy\'s', 'Dillard\'s'],
    "Robes": ['Victoria\'s Secret', 'PINK', 'Aerie', 'Calvin Klein', 'Natori', 'Eberjey', 'Parachute', 'Boll & Branch', 'Brooklinen', 'Pottery Barn', 'Restoration Hardware', 'Nordstrom', 'Bloomingdale\'s'],
    "Swimwear": ['Speedo', 'TYR', 'Arena', 'Billabong', 'Roxy', 'Quiksilver', 'Volcom', 'Hurley', 'O\'Neill', 'Seafolly', 'Zimmermann', 'Solid & Striped', 'Vitamin A', 'Beach Riot', 'L*Space', 'Maaji', 'La Blanca', 'Catalina', 'Venus', 'Lands\' End'],
    "Beachwear": ['Billabong', 'Roxy', 'Quiksilver', 'Volcom', 'Hurley', 'O\'Neill', 'Seafolly', 'Zimmermann', 'Solid & Striped', 'Vitamin A', 'Beach Riot', 'L*Space', 'Maaji', 'La Blanca', 'Catalina', 'Venus', 'Lands\' End', 'Tommy Bahama', 'H&M', 'Zara', 'ASOS'],

    # Footwear
    "Sneakers": ['Nike', 'Adidas', 'Puma', 'Reebok', 'New Balance', 'ASICS', 'Converse', 'Vans', 'Fila', 'Skechers', 'Under Armour', 'Jordan', 'Yeezy', 'Balenciaga', 'Gucci', 'Prada', 'Saint Laurent', 'Alexander McQueen', 'Common Projects', 'Veja'],
    "Running Shoes": ['Nike', 'Adidas', 'Puma', 'Reebok', 'New Balance', 'ASICS', 'Brooks', 'Saucony', 'Hoka One One', 'Mizuno', 'Altra', 'On Running', 'Under Armour', 'Skechers'],
    "Sandals": ['Birkenstock', 'Teva', 'Chaco', 'Crocs', 'Havaianas', 'Rainbow', 'Olukai', 'ECCO', 'Clarks', 'Naturalizer', 'Sam Edelman', 'Steve Madden', 'Tory Burch', 'Michael Kors', 'Gucci', 'Prada', 'Valentino', 'Hermès', 'Tory Burch'],
    "Flip Flops": ['Havaianas', 'Rainbow', 'Reef', 'Olukai', 'Crocs', 'Adidas', 'Nike', 'Puma', 'Teva', 'Tommy Bahama', 'Sperry', 'UGG', 'Tory Burch', 'Michael Kors', 'Kate Spade'],
    "Heels": ['Christian Louboutin', 'Jimmy Choo', 'Manolo Blahnik', 'Stuart Weitzman', 'Tory Burch', 'Sam Edelman', 'Steve Madden', 'Nine West', 'Jessica Simpson', 'Michael Kors', 'Gucci', 'Prada', 'Valentino', 'Gianvito Rossi', 'Aquazzura', 'Badgley Mischka'],
    "Pumps": ['Christian Louboutin', 'Jimmy Choo', 'Manolo Blahnik', 'Stuart Weitzman', 'Tory Burch', 'Sam Edelman', 'Steve Madden', 'Nine West', 'Jessica Simpson', 'Michael Kors', 'Gucci', 'Prada', 'Valentino', 'Gianvito Rossi', 'Aquazzura', 'Badgley Mischka'],
    "Boots": ['Dr. Martens', 'Timberland', 'UGG', 'Frye', 'Hunter', 'Sorel', 'Blundstone', 'Clarks', 'Steve Madden', 'Sam Edelman', 'Michael Kors', 'Tory Burch', 'Gucci', 'Prada', 'Saint Laurent', 'Alexander Wang', 'Chanel', 'Dolce & Gabbana'],
    "Loafers": ['Gucci', 'Prada', 'Tod\'s', 'Salvatore Ferragamo', 'Bally', 'Cole Haan', 'Bass', 'Sperry', 'Sebago', 'Clarks', 'ECCO', 'Rockport', 'Sam Edelman', 'Michael Kors', 'Tory Burch', 'Banana Republic', 'J.Crew'],
    "Slippers": ['UGG', 'Minnetonka', 'L.L.Bean', 'Sperry', 'Crocs', 'Isotoner', 'Acorn', 'Dearfoams', 'Haflinger', 'Glerups', 'Birkenstock', 'Gucci', 'Prada', 'Burberry', 'Tory Burch', 'Michael Kors'],
    "Formal Shoes": ['Allen Edmonds', 'Johnston & Murphy', 'Cole Haan', 'Florsheim', 'Alden', 'Church\'s', 'Salvatore Ferragamo', 'Bally', 'Tod\'s', 'Bruno Magli', 'Magnanni', 'Hugo Boss', 'Calvin Klein', 'Kenneth Cole', 'Rockport', 'ECCO', 'Clarks'],

    # Accessories
    "Watches": ['Rolex', 'Omega', 'Tag Heuer', 'Breitling', 'Longines', 'Tissot', 'Cartier', 'Patek Philippe', 'Audemars Piguet', 'IWC', 'Panerai', 'Hublot', 'Jaeger-LeCoultre', 'Vacheron Constantin', 'Seiko', 'Citizen', 'Casio', 'Timex', 'Fossil', 'Michael Kors', 'Daniel Wellington', 'MVMT', 'Apple'],
    "Sunglasses": ['Ray-Ban', 'Oakley', 'Maui Jim', 'Persol', 'Carrera', 'Prada', 'Gucci', 'Dior', 'Chanel', 'Versace', 'Dolce & Gabbana', 'Tom Ford', 'Saint Laurent', 'Bvlgari', 'Burberry', 'Fendi', 'Valentino', 'Armani', 'Coach', 'Michael Kors', 'Kate Spade', 'Tory Burch'],
    "Handbags": ['Louis Vuitton', 'Gucci', 'Chanel', 'Hermès', 'Prada', 'Dior', 'Fendi', 'Saint Laurent', 'Bottega Veneta', 'Burberry', 'Givenchy', 'Balenciaga', 'Valentino', 'Celine', 'Loewe', 'Miu Miu', 'Chloé', 'Coach', 'Michael Kors', 'Kate Spade', 'Tory Burch', 'Longchamp', 'Mansur Gavriel'],
    "Wallets": ['Louis Vuitton', 'Gucci', 'Chanel', 'Prada', 'Dior', 'Fendi', 'Saint Laurent', 'Bottega Veneta', 'Burberry', 'Givenchy', 'Balenciaga', 'Coach', 'Michael Kors', 'Kate Spade', 'Tory Burch', 'Fossil', 'Herschel', 'Bellroy', 'Secrid', 'Ridge'],
    "Backpacks": ['North Face', 'JanSport', 'Herschel', 'Eastpak', 'Fjällräven', 'Patagonia', 'Osprey', 'Deuter', 'Gregory', 'MCM', 'Prada', 'Gucci', 'Dior', 'Fendi', 'Saint Laurent', 'Burberry', 'Herschel', 'Tumi', 'Samsonite', 'Swissgear', 'Under Armour', 'Nike', 'Adidas'],
    "Belts": ['Gucci', 'Louis Vuitton', 'Hermès', 'Prada', 'Dior', 'Fendi', 'Saint Laurent', 'Bottega Veneta', 'Burberry', 'Salvatore Ferragamo', 'Dolce & Gabbana', 'Versace', 'Armani', 'Hugo Boss', 'Tommy Hilfiger', 'Ralph Lauren', 'Calvin Klein', 'Coach', 'Michael Kors', 'Kate Spade', 'Tory Burch'],
	"Rings": ['Gucci', 'Louis Vuitton', 'Hermès', 'Prada', 'Dior', 'Fendi', 'Saint Laurent', 'Bottega Veneta', 'Burberry', 'Salvatore Ferragamo', 'Dolce & Gabbana', 'Versace', 'Armani', 'Hugo Boss', 'Tommy Hilfiger', 'Ralph Lauren', 'Calvin Klein', 'Coach', 'Michael Kors', 'Kate Spade', 'Tory Burch'],
    "Hats": ['New Era', 'Brixton', 'Stetson', 'Bailey', 'Ebbets Field', 'Nike', 'Adidas', 'Puma', 'The North Face', 'Patagonia', 'Gucci', 'Prada', 'Dior', 'Fendi', 'Saint Laurent', 'Burberry', 'Ralph Lauren', 'Tommy Hilfiger', 'Calvin Klein'],
    "Caps": ['New Era', '47 Brand', 'Nike', 'Adidas', 'Puma', 'Under Armour', 'The North Face', 'Patagonia', 'Carhartt', 'Dickies', 'Ralph Lauren', 'Tommy Hilfiger', 'Calvin Klein', 'Gucci', 'Prada', 'Dior', 'Fendi', 'Balenciaga', 'Off-White', 'Supreme'],
    "Gloves": ['The North Face', 'Patagonia', 'Columbia', 'Arc\'teryx', 'Marmot', 'Black Diamond', 'Burton', 'Hestra', 'Wells Lamont', 'Carhartt', 'Isotoner', 'Coach', 'Michael Kors', 'Kate Spade', 'Tory Burch', 'Gucci', 'Prada', 'Dior', 'Fendi', 'Saint Laurent'],
    "Ties": ['Hugo Boss', 'Armani', 'Zegna', 'Ralph Lauren', 'Tommy Hilfiger', 'Calvin Klein', 'Brooks Brothers', 'J.Crew', 'Banana Republic', 'Gucci', 'Prada', 'Dior', 'Fendi', 'Saint Laurent', 'Burberry', 'Dolce & Gabbana', 'Versace', 'Hermès', 'Ferragamo', 'Brioni'],

    # Cosmetics
    "Lipstick": ['MAC Cosmetics', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'NYX', 'Urban Decay', 'Too Faced', 'Anastasia Beverly Hills', 'Fenty Beauty', 'Huda Beauty', 'Charlotte Tilbury', 'NARS', 'YSL Beauty', 'Dior', 'Chanel', 'Gucci', 'Tom Ford', 'Estée Lauder', 'Clinique'],
    "Lip Gloss": ['Fenty Beauty', 'Glossier', 'NYX', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'MAC Cosmetics', 'Urban Decay', 'Too Faced', 'Anastasia Beverly Hills', 'Huda Beauty', 'Charlotte Tilbury', 'NARS', 'Dior', 'Chanel', 'YSL Beauty', 'Tom Ford', 'Estée Lauder', 'Clinique'],
    "Lip Balm": ['Burt\'s Bees', 'EOS', 'Carmex', 'Nivea', 'ChapStick', 'Glossier', 'Fresh', 'Kiehl\'s', 'Jack Black', 'Aquaphor', 'Laneige', 'Vaseline', 'Dr. Bronner\'s', 'The Body Shop', 'Blistex', 'Palmer\'s', 'Badger', 'Hurraw!', 'Mentholatum'],
    "Foundation": ['Estée Lauder', 'L\'Oréal Paris', 'Maybelline', 'Revlon', 'CoverGirl', 'MAC Cosmetics', 'Fenty Beauty', 'NARS', 'Make Up For Ever', 'Dior', 'Chanel', 'Lancôme', 'Giorgio Armani', 'YSL Beauty', 'Too Faced', 'Huda Beauty', 'IT Cosmetics', 'Tarte', 'Urban Decay', 'Clinique'],
    "Concealer": ['Tarte', 'NARS', 'Maybelline', 'L\'Oréal Paris', 'MAC Cosmetics', 'Estée Lauder', 'Too Faced', 'Fenty Beauty', 'Huda Beauty', 'IT Cosmetics', 'Urban Decay', 'NYX', 'Revlon', 'CoverGirl', 'Dior', 'Chanel', 'Lancôme', 'YSL Beauty', 'Clinique', 'Bobbi Brown'],
    "Compact Powder": ['MAC Cosmetics', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'Estée Lauder', 'Chanel', 'Dior', 'Lancôme', 'YSL Beauty', 'NARS', 'Too Faced', 'Fenty Beauty', 'Huda Beauty', 'IT Cosmetics', 'Urban Decay', 'NYX', 'Clinique', 'Bobbi Brown'],
    "Highlighter": ['Fenty Beauty', 'Becca', 'Anastasia Beverly Hills', 'MAC Cosmetics', 'NARS', 'Too Faced', 'Urban Decay', 'NYX', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'Dior', 'Chanel', 'YSL Beauty', 'Estée Lauder', 'Clinique', 'Bobbi Brown', 'Huda Beauty', 'Charlotte Tilbury'],
    "Blush": ['NARS', 'MAC Cosmetics', 'Tarte', 'Benefit Cosmetics', 'Too Faced', 'Urban Decay', 'NYX', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'Dior', 'Chanel', 'YSL Beauty', 'Estée Lauder', 'Clinique', 'Bobbi Brown', 'Milani', 'Physicians Formula', 'e.l.f.'],
    "Primer": ['Smashbox', 'Benefit Cosmetics', 'Too Faced', 'Urban Decay', 'NYX', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'MAC Cosmetics', 'Estée Lauder', 'Fenty Beauty', 'Huda Beauty', 'IT Cosmetics', 'Tarte', 'Dior', 'Chanel', 'YSL Beauty', 'Clinique', 'Bobbi Brown'],
    "Setting Spray": ['Urban Decay', 'MAC Cosmetics', 'NYX', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'Estée Lauder', 'Fenty Beauty', 'Huda Beauty', 'Morphe', 'Milani', 'e.l.f.', 'Skindinavia', 'Benefit Cosmetics', 'Too Faced', 'Tarte', 'Dior', 'Chanel', 'YSL Beauty'],
    "Mascara": ['Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'MAC Cosmetics', 'Too Faced', 'Benefit Cosmetics', 'Urban Decay', 'Tarte', 'Estée Lauder', 'Dior', 'Chanel', 'Lancôme', 'YSL Beauty', 'Clinique', 'Bobbi Brown', 'Fenty Beauty', 'Huda Beauty', 'IT Cosmetics', 'NYX'],
    "Eyeliner": ['Stila', 'Kat Von D', 'Urban Decay', 'NYX', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'MAC Cosmetics', 'Too Faced', 'Benefit Cosmetics', 'Tarte', 'Estée Lauder', 'Dior', 'Chanel', 'Lancôme', 'YSL Beauty', 'Clinique', 'Bobbi Brown', 'Fenty Beauty'],
    "Eyeshadow": ['Urban Decay', 'Anastasia Beverly Hills', 'Too Faced', 'Tarte', 'NYX', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'MAC Cosmetics', 'Estée Lauder', 'Dior', 'Chanel', 'Lancôme', 'YSL Beauty', 'Clinique', 'Bobbi Brown', 'Fenty Beauty', 'Huda Beauty', 'Natasha Denona'],
    "Kohl": ['Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'MAC Cosmetics', 'NYX', 'Urban Decay', 'Too Faced', 'Benefit Cosmetics', 'Tarte', 'Estée Lauder', 'Dior', 'Chanel', 'Lancôme', 'YSL Beauty', 'Clinique', 'Bobbi Brown', 'Fenty Beauty', 'Huda Beauty', 'IT Cosmetics'],
    "Eyebrow Pencil": ['Anastasia Beverly Hills', 'Benefit Cosmetics', 'NYX', 'Maybelline', 'L\'Oréal Paris', 'Revlon', 'CoverGirl', 'MAC Cosmetics', 'Urban Decay', 'Too Faced', 'Tarte', 'Estée Lauder', 'Dior', 'Chanel', 'Lancôme', 'YSL Beauty', 'Clinique', 'Bobbi Brown', 'Fenty Beauty', 'Huda Beauty'],
    "Makeup Remover": ['Neutrogena', 'Garnier', 'Bioderma', 'L\'Oréal Paris', 'Maybelline', 'Clinique', 'Estée Lauder', 'Dior', 'Chanel', 'Lancôme', 'YSL Beauty', 'MAC Cosmetics', 'Sephora Collection', 'The Body Shop', 'Simple', 'Cetaphil', 'Aveeno', 'Eucerin', 'La Roche-Posay', 'Vichy'],
    "Nail Polish": ['OPI', 'Essie', 'Sally Hansen', 'Revlon', 'Maybelline', 'L\'Oréal Paris', 'CoverGirl', 'China Glaze', 'Zoya', 'Butter London', 'Deborah Lippmann', 'Chanel', 'Dior', 'YSL Beauty', 'Tom Ford', 'Nails Inc', 'ORLY', 'CND', 'Smith & Cult', 'KL Polish'],
    "Nail Polish Remover": ['OPI', 'Essie', 'Sally Hansen', 'Revlon', 'Maybelline', 'L\'Oréal Paris', 'Cutex', 'Zoya', 'Butter London', 'Deborah Lippmann', 'CND', 'Orly', 'Nails Inc', 'Seche', 'Onsen', 'Priti NYC', 'SpaRitual', 'Tenoverten', 'Smith & Cult'],
    "Makeup Brushes": ['Morphe', 'Sigma', 'Real Techniques', 'EcoTools', 'Zoeva', 'MAC Cosmetics', 'Sephora Collection', 'BH Cosmetics', 'Anastasia Beverly Hills', 'IT Cosmetics', 'Tarte', 'Too Faced', 'Urban Decay', 'Fenty Beauty', 'Huda Beauty', 'Elf', 'Wet n Wild', 'ColourPop', 'Juvia\'s Place', 'Makeup Geek'],
    "Beauty Blender": ['Beautyblender', 'Real Techniques', 'EcoTools', 'Sigma', 'Morphe', 'Sephora Collection', 'Elf', 'Wet n Wild', 'NYX', 'Tarte', 'Too Faced', 'Urban Decay', 'Fenty Beauty', 'Huda Beauty', 'IT Cosmetics', 'Make Up For Ever', 'L\'Oréal Paris', 'Maybelline', 'Revlon', 'CoverGirl'],

    # Skincare
    "Face Wash": ['Cetaphil', 'CeraVe', 'Neutrogena', 'La Roche-Posay', 'Aveeno', 'Garnier', 'L\'Oréal Paris', 'Simple', 'Bioderma', 'Vichy', 'Eucerin', 'Dove', 'Nivea', 'Olay', 'Clean & Clear', 'St. Ives', 'Philosophy', 'Fresh', 'Kiehl\'s', 'Clinique'],
    "Face Scrub": ['St. Ives', 'Neutrogena', 'Clean & Clear', 'Olay', 'L\'Oréal Paris', 'Garnier', 'Simple', 'Aveeno', 'Cetaphil', 'CeraVe', 'La Roche-Posay', 'Bioderma', 'Vichy', 'Eucerin', 'Dove', 'Nivea', 'Philosophy', 'Fresh', 'Kiehl\'s', 'Clinique'],
    "Moisturizer": ['CeraVe', 'Cetaphil', 'Neutrogena', 'La Roche-Posay', 'Aveeno', 'Garnier', 'L\'Oréal Paris', 'Simple', 'Bioderma', 'Vichy', 'Eucerin', 'Dove', 'Nivea', 'Olay', 'Clean & Clear', 'St. Ives', 'Philosophy', 'Fresh', 'Kiehl\'s', 'Clinique'],
    "Sunscreen": ['Neutrogena', 'La Roche-Posay', 'Aveeno', 'CeraVe', 'Cetaphil', 'Garnier', 'L\'Oréal Paris', 'Bioderma', 'Vichy', 'Eucerin', 'Dove', 'Nivea', 'Olay', 'Banana Boat', 'Coppertone', 'Supergoop!', 'EltaMD', 'Blue Lizard', 'Coola', 'Shiseido'],
    "Face Mask": ['L\'Oréal Paris', 'Garnier', 'Neutrogena', 'Olay', 'Simple', 'Aveeno', 'Cetaphil', 'CeraVe', 'La Roche-Posay', 'Bioderma', 'Vichy', 'Eucerin', 'Dove', 'Nivea', 'The Body Shop', 'Fresh', 'Kiehl\'s', 'Clinique', 'Origins', 'Glossier'],
    "Face Serum": ['The Ordinary', 'CeraVe', 'La Roche-Posay', 'Vichy', 'Neutrogena', 'Olay', 'L\'Oréal Paris', 'Garnier', 'Simple', 'Aveeno', 'Cetaphil', 'Bioderma', 'Eucerin', 'Dove', 'Nivea', 'The Body Shop', 'Fresh', 'Kiehl\'s', 'Clinique', 'Estée Lauder'],
    "Night Cream": ['CeraVe', 'Cetaphil', 'Neutrogena', 'La Roche-Posay', 'Aveeno', 'Garnier', 'L\'Oréal Paris', 'Simple', 'Bioderma', 'Vichy', 'Eucerin', 'Dove', 'Nivea', 'Olay', 'The Body Shop']}


colors = ["Red", "Blue", "Green", "Black", "White", "Yellow",
          "Red-Black", "White-Black", "White-Yellow", "Magenta", "Gold", "Silver", "Gray", "Metal" ]
sizes = ["XS", "S", "M", "L", "XL"]
# ring_sizes = np.arange(16,22)
# watch_sizes = np.arange(10,16)

def random_date():
    start_date = datetime.now() - timedelta(days=365)
    return start_date + timedelta(days=random.randint(0, 365))

# SQLite setup
conn = sqlite3.connect('OMS.db')
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT,
    name TEXT,
    item_type TEXT,
    color TEXT,
    size TEXT,
    price REAL,
    vendor TEXT,
    quantity_in_stock INTEGER,
    rating REAL,
    times_purchased INTEGER,
    active INTEGER,
    created_at TEXT,
    updated_at TEXT
)
''')

not_included = ['Rings']
fancy_terms = [
    None, "Brand New", "Exclusive", "Fascinating", "Limited Edition", "Premium Quality", None,
    "Best Seller", "Trending Now", "Editor’s Pick", "Customer Favorite", "Handcrafted", None,
    "Artisan Made", "Signature Design", "Ultra Soft", "Luxury Finish", "Top Rated", None,
    "Award-Winning", "Eco-Friendly", "Made with Love", "One of a Kind", "Just Arrived", None,
    "Limited Stock", "Hot Item", "Fan Favorite", "Modern Classic", "Essential Pick", None,
    "Curated Style", "Bold Look", "Instant Classic",None,None, "All-Time Favorite", "Highly Recommended"
]

def get_or_create_id(table, name):
    cursor.execute(f"SELECT id FROM {table} WHERE LOWER(name) = LOWER(?)", (name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(f"INSERT INTO {table} (name) VALUES (?)", (name,))
    conn.commit()
    return cursor.lastrowid

product_types = list(product_vendors.keys())
for _ in range(100000):
    product_type = random.choice(product_types)
    vendor = random.choice(product_vendors[product_type])
    color = random.choice(colors) if product_type not in not_included else ''
    size = random.choice(sizes)
    price = round(random.uniform(10.0, 500.0), 2)
    quantity_in_stock = random.randint(10, 1000)
    rating = 0
    times_purchased = 0
    active = 1
    created_at = random_date()
    updated_at = created_at + timedelta(days=random.randint(0, 30))
    # sku = str(uuid.uuid4())[:8]
    term = random.choice(fancy_terms)
    
    name_parts = [term, vendor, product_type, color, size]
    name = " ".join([part for part in name_parts if part]).strip()
    sku = str(hash(name))[:8]
    
    vendor_id = get_or_create_id("vendors", vendor)
    item_type_id = get_or_create_id("item_types", product_type)

    # Insert into items table
    try:
        cursor.execute("""
            INSERT INTO items (
                name, sku, vendor_id, item_type_id, color, size,
                price, quantity_in_stock, rating, times_purchased,
                active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name, sku, vendor_id, item_type_id, color, size,
            price, quantity_in_stock, rating, times_purchased,
            active, created_at.strftime("%Y-%m-%d %H:%M:%S"),
            updated_at.strftime("%Y-%m-%d %H:%M:%S")
        ))
    except Exception as e:
        pass

# Commit and close
conn.commit()
conn.close()