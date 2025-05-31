import sqlite3
import random
from datetime import datetime, timedelta

# Database connection
def connect_db(db_path='OMS.db'):
    """Connect to SQLite database"""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

# Countries data (100 countries with ISO codes)
COUNTRIES_DATA = [
    ('Afghanistan', 'AF'), ('Albania', 'AL'), ('Algeria', 'DZ'), ('Argentina', 'AR'),
    ('Armenia', 'AM'), ('Australia', 'AU'), ('Austria', 'AT'), ('Azerbaijan', 'AZ'),
    ('Bahrain', 'BH'), ('Bangladesh', 'BD'), ('Belarus', 'BY'), ('Belgium', 'BE'),
    ('Bolivia', 'BO'), ('Bosnia and Herzegovina', 'BA'), ('Brazil', 'BR'), ('Bulgaria', 'BG'),
    ('Cambodia', 'KH'), ('Canada', 'CA'), ('Chile', 'CL'), ('China', 'CN'),
    ('Colombia', 'CO'), ('Croatia', 'HR'), ('Czech Republic', 'CZ'), ('Denmark', 'DK'),
    ('Ecuador', 'EC'), ('Egypt', 'EG'), ('Estonia', 'EE'), ('Ethiopia', 'ET'),
    ('Finland', 'FI'), ('France', 'FR'), ('Georgia', 'GE'), ('Germany', 'DE'),
    ('Ghana', 'GH'), ('Greece', 'GR'), ('Guatemala', 'GT'), ('Hungary', 'HU'),
    ('Iceland', 'IS'), ('India', 'IN'), ('Indonesia', 'ID'), ('Iran', 'IR'),
    ('Iraq', 'IQ'), ('Ireland', 'IE'), ('Israel', 'IL'), ('Italy', 'IT'),
    ('Jamaica', 'JM'), ('Japan', 'JP'), ('Jordan', 'JO'), ('Kazakhstan', 'KZ'),
    ('Kenya', 'KE'), ('Kuwait', 'KW'), ('Latvia', 'LV'), ('Lebanon', 'LB'),
    ('Libya', 'LY'), ('Lithuania', 'LT'), ('Luxembourg', 'LU'), ('Malaysia', 'MY'),
    ('Malta', 'MT'), ('Mexico', 'MX'), ('Moldova', 'MD'), ('Mongolia', 'MN'),
    ('Morocco', 'MA'), ('Myanmar', 'MM'), ('Nepal', 'NP'), ('Netherlands', 'NL'),
    ('New Zealand', 'NZ'), ('Nigeria', 'NG'), ('North Korea', 'KP'), ('Norway', 'NO'),
    ('Oman', 'OM'), ('Pakistan', 'PK'), ('Panama', 'PA'), ('Paraguay', 'PY'),
    ('Peru', 'PE'), ('Philippines', 'PH'), ('Poland', 'PL'), ('Portugal', 'PT'),
    ('Qatar', 'QA'), ('Romania', 'RO'), ('Russia', 'RU'), ('Saudi Arabia', 'SA'),
    ('Senegal', 'SN'), ('Serbia', 'RS'), ('Singapore', 'SG'), ('Slovakia', 'SK'),
    ('Slovenia', 'SI'), ('South Africa', 'ZA'), ('South Korea', 'KR'), ('Spain', 'ES'),
    ('Sri Lanka', 'LK'), ('Sudan', 'SD'), ('Sweden', 'SE'), ('Switzerland', 'CH'),
    ('Syria', 'SY'), ('Taiwan', 'TW'), ('Tajikistan', 'TJ'), ('Tanzania', 'TZ'),
    ('Thailand', 'TH'), ('Tunisia', 'TN'), ('Turkey', 'TR'), ('Ukraine', 'UA'),
    ('United Arab Emirates', 'AE'), ('United Kingdom', 'GB'), ('United States', 'US'), ('Uruguay', 'UY'),
    ('Uzbekistan', 'UZ'), ('Venezuela', 'VE'), ('Vietnam', 'VN'), ('Yemen', 'YE'),
    ('Zambia', 'ZM'), ('Zimbabwe', 'ZW')
]

# Cities data mapped to countries (500+ cities)
CITIES_DATA = {
    'United States': [
        ('New York', 'New York'), ('Los Angeles', 'California'), ('Chicago', 'Illinois'),
        ('Houston', 'Texas'), ('Phoenix', 'Arizona'), ('Philadelphia', 'Pennsylvania'),
        ('San Antonio', 'Texas'), ('San Diego', 'California'), ('Dallas', 'Texas'),
        ('San Jose', 'California'), ('Austin', 'Texas'), ('Jacksonville', 'Florida'),
        ('Fort Worth', 'Texas'), ('Columbus', 'Ohio'), ('Charlotte', 'North Carolina'),
        ('San Francisco', 'California'), ('Indianapolis', 'Indiana'), ('Seattle', 'Washington'),
        ('Denver', 'Colorado'), ('Washington', 'District of Columbia'), ('Boston', 'Massachusetts'),
        ('El Paso', 'Texas'), ('Nashville', 'Tennessee'), ('Detroit', 'Michigan'),
        ('Oklahoma City', 'Oklahoma'), ('Portland', 'Oregon'), ('Las Vegas', 'Nevada'),
        ('Memphis', 'Tennessee'), ('Louisville', 'Kentucky'), ('Baltimore', 'Maryland'),
        ('Milwaukee', 'Wisconsin'), ('Albuquerque', 'New Mexico'), ('Tucson', 'Arizona'),
        ('Fresno', 'California'), ('Mesa', 'Arizona'), ('Sacramento', 'California'),
        ('Atlanta', 'Georgia'), ('Kansas City', 'Missouri'), ('Colorado Springs', 'Colorado'),
        ('Miami', 'Florida'), ('Raleigh', 'North Carolina'), ('Omaha', 'Nebraska'),
        ('Long Beach', 'California'), ('Virginia Beach', 'Virginia'), ('Oakland', 'California')
    ],
    'United Kingdom': [
        ('London', 'England'), ('Birmingham', 'England'), ('Manchester', 'England'),
        ('Glasgow', 'Scotland'), ('Liverpool', 'England'), ('Edinburgh', 'Scotland'),
        ('Leeds', 'England'), ('Sheffield', 'England'), ('Bristol', 'England'),
        ('Cardiff', 'Wales'), ('Belfast', 'Northern Ireland'), ('Leicester', 'England'),
        ('Coventry', 'England'), ('Bradford', 'England'), ('Nottingham', 'England'),
        ('Kingston upon Hull', 'England'), ('Plymouth', 'England'), ('Stoke-on-Trent', 'England'),
        ('Wolverhampton', 'England'), ('Derby', 'England'), ('Southampton', 'England'),
        ('Portsmouth', 'England'), ('Newcastle upon Tyne', 'England'), ('Brighton', 'England')
    ],
    'Canada': [
        ('Toronto', 'Ontario'), ('Montreal', 'Quebec'), ('Vancouver', 'British Columbia'),
        ('Calgary', 'Alberta'), ('Edmonton', 'Alberta'), ('Ottawa', 'Ontario'),
        ('Winnipeg', 'Manitoba'), ('Quebec City', 'Quebec'), ('Hamilton', 'Ontario'),
        ('Kitchener', 'Ontario'), ('London', 'Ontario'), ('Victoria', 'British Columbia'),
        ('Halifax', 'Nova Scotia'), ('Oshawa', 'Ontario'), ('Windsor', 'Ontario'),
        ('Saskatoon', 'Saskatchewan'), ('Regina', 'Saskatchewan'), ('St. Catharines', 'Ontario')
    ],
    'Germany': [
        ('Berlin', 'Berlin'), ('Hamburg', 'Hamburg'), ('Munich', 'Bavaria'),
        ('Cologne', 'North Rhine-Westphalia'), ('Frankfurt', 'Hesse'), ('Stuttgart', 'Baden-Württemberg'),
        ('Düsseldorf', 'North Rhine-Westphalia'), ('Leipzig', 'Saxony'), ('Dortmund', 'North Rhine-Westphalia'),
        ('Essen', 'North Rhine-Westphalia'), ('Bremen', 'Bremen'), ('Dresden', 'Saxony'),
        ('Hanover', 'Lower Saxony'), ('Nuremberg', 'Bavaria'), ('Duisburg', 'North Rhine-Westphalia'),
        ('Bochum', 'North Rhine-Westphalia'), ('Wuppertal', 'North Rhine-Westphalia'), ('Bonn', 'North Rhine-Westphalia')
    ],
    'France': [
        ('Paris', 'Île-de-France'), ('Marseille', 'Provence-Alpes-Côte d\'Azur'), ('Lyon', 'Auvergne-Rhône-Alpes'),
        ('Toulouse', 'Occitanie'), ('Nice', 'Provence-Alpes-Côte d\'Azur'), ('Nantes', 'Pays de la Loire'),
        ('Montpellier', 'Occitanie'), ('Strasbourg', 'Grand Est'), ('Bordeaux', 'Nouvelle-Aquitaine'),
        ('Lille', 'Hauts-de-France'), ('Rennes', 'Brittany'), ('Reims', 'Grand Est'),
        ('Saint-Étienne', 'Auvergne-Rhône-Alpes'), ('Le Havre', 'Normandy'), ('Toulon', 'Provence-Alpes-Côte d\'Azur')
    ],
    'India': [
        ('Mumbai', 'Maharashtra'), ('Delhi', 'Delhi'), ('Bangalore', 'Karnataka'),
        ('Hyderabad', 'Telangana'), ('Ahmedabad', 'Gujarat'), ('Chennai', 'Tamil Nadu'),
        ('Kolkata', 'West Bengal'), ('Surat', 'Gujarat'), ('Pune', 'Maharashtra'),
        ('Jaipur', 'Rajasthan'), ('Lucknow', 'Uttar Pradesh'), ('Kanpur', 'Uttar Pradesh'),
        ('Nagpur', 'Maharashtra'), ('Indore', 'Madhya Pradesh'), ('Thane', 'Maharashtra'),
        ('Bhopal', 'Madhya Pradesh'), ('Visakhapatnam', 'Andhra Pradesh'), ('Pimpri-Chinchwad', 'Maharashtra'),
        ('Patna', 'Bihar'), ('Vadodara', 'Gujarat'), ('Ghaziabad', 'Uttar Pradesh'),
        ('Ludhiana', 'Punjab'), ('Agra', 'Uttar Pradesh'), ('Nashik', 'Maharashtra'),
        ('Faridabad', 'Haryana'), ('Meerut', 'Uttar Pradesh'), ('Rajkot', 'Gujarat'),
        ('Kalyan-Dombivli', 'Maharashtra'), ('Vasai-Virar', 'Maharashtra'), ('Varanasi', 'Uttar Pradesh')
    ],
    'China': [
        ('Shanghai', 'Shanghai'), ('Beijing', 'Beijing'), ('Chongqing', 'Chongqing'),
        ('Tianjin', 'Tianjin'), ('Guangzhou', 'Guangdong'), ('Shenzhen', 'Guangdong'),
        ('Wuhan', 'Hubei'), ('Dongguan', 'Guangdong'), ('Chengdu', 'Sichuan'),
        ('Nanjing', 'Jiangsu'), ('Foshan', 'Guangdong'), ('Shenyang', 'Liaoning'),
        ('Hangzhou', 'Zhejiang'), ('Xi\'an', 'Shaanxi'), ('Harbin', 'Heilongjiang'),
        ('Suzhou', 'Jiangsu'), ('Qingdao', 'Shandong'), ('Dalian', 'Liaoning'),
        ('Zhengzhou', 'Henan'), ('Shantou', 'Guangdong'), ('Jinan', 'Shandong'),
        ('Changchun', 'Jilin'), ('Kunming', 'Yunnan'), ('Changsha', 'Hunan'),
        ('Taiyuan', 'Shanxi'), ('Xiamen', 'Fujian'), ('Shijiazhuang', 'Hebei')
    ],
    'Japan': [
        ('Tokyo', 'Tokyo'), ('Yokohama', 'Kanagawa'), ('Osaka', 'Osaka'),
        ('Nagoya', 'Aichi'), ('Sapporo', 'Hokkaido'), ('Fukuoka', 'Fukuoka'),
        ('Kobe', 'Hyogo'), ('Kawasaki', 'Kanagawa'), ('Kyoto', 'Kyoto'),
        ('Saitama', 'Saitama'), ('Hiroshima', 'Hiroshima'), ('Sendai', 'Miyagi'),
        ('Kitakyushu', 'Fukuoka'), ('Chiba', 'Chiba'), ('Sakai', 'Osaka'),
        ('Niigata', 'Niigata'), ('Hamamatsu', 'Shizuoka'), ('Okayama', 'Okayama')
    ],
    'Brazil': [
        ('São Paulo', 'São Paulo'), ('Rio de Janeiro', 'Rio de Janeiro'), ('Brasília', 'Federal District'),
        ('Fortaleza', 'Ceará'), ('Belo Horizonte', 'Minas Gerais'), ('Manaus', 'Amazonas'),
        ('Curitiba', 'Paraná'), ('Recife', 'Pernambuco'), ('Porto Alegre', 'Rio Grande do Sul'),
        ('Belém', 'Pará'), ('Goiânia', 'Goiás'), ('Guarulhos', 'São Paulo'),
        ('Campinas', 'São Paulo'), ('São Luís', 'Maranhão'), ('São Gonçalo', 'Rio de Janeiro'),
        ('Maceió', 'Alagoas'), ('Duque de Caxias', 'Rio de Janeiro'), ('Natal', 'Rio Grande do Norte')
    ],
    'Australia': [
        ('Sydney', 'New South Wales'), ('Melbourne', 'Victoria'), ('Brisbane', 'Queensland'),
        ('Perth', 'Western Australia'), ('Adelaide', 'South Australia'), ('Gold Coast', 'Queensland'),
        ('Newcastle', 'New South Wales'), ('Canberra', 'Australian Capital Territory'), ('Sunshine Coast', 'Queensland'),
        ('Wollongong', 'New South Wales'), ('Hobart', 'Tasmania'), ('Geelong', 'Victoria'),
        ('Townsville', 'Queensland'), ('Cairns', 'Queensland'), ('Darwin', 'Northern Territory')
    ],
    'Italy': [
        ('Rome', 'Lazio'), ('Milan', 'Lombardy'), ('Naples', 'Campania'),
        ('Turin', 'Piedmont'), ('Palermo', 'Sicily'), ('Genoa', 'Liguria'),
        ('Bologna', 'Emilia-Romagna'), ('Florence', 'Tuscany'), ('Bari', 'Apulia'),
        ('Catania', 'Sicily'), ('Venice', 'Veneto'), ('Verona', 'Veneto'),
        ('Messina', 'Sicily'), ('Padua', 'Veneto'), ('Trieste', 'Friuli-Venezia Giulia')
    ],
    'Spain': [
        ('Madrid', 'Community of Madrid'), ('Barcelona', 'Catalonia'), ('Valencia', 'Valencian Community'),
        ('Seville', 'Andalusia'), ('Zaragoza', 'Aragon'), ('Málaga', 'Andalusia'),
        ('Murcia', 'Region of Murcia'), ('Palma', 'Balearic Islands'), ('Las Palmas', 'Canary Islands'),
        ('Bilbao', 'Basque Country'), ('Alicante', 'Valencian Community'), ('Córdoba', 'Andalusia'),
        ('Valladolid', 'Castile and León'), ('Vigo', 'Galicia'), ('Gijón', 'Asturias')
    ],
    'Mexico': [
        ('Mexico City', 'Mexico City'), ('Guadalajara', 'Jalisco'), ('Monterrey', 'Nuevo León'),
        ('Puebla', 'Puebla'), ('Tijuana', 'Baja California'), ('León', 'Guanajuato'),
        ('Juárez', 'Chihuahua'), ('Torreón', 'Coahuila'), ('Querétaro', 'Querétaro'),
        ('San Luis Potosí', 'San Luis Potosí'), ('Mérida', 'Yucatán'), ('Mexicali', 'Baja California'),
        ('Aguascalientes', 'Aguascalientes'), ('Cuernavaca', 'Morelos'), ('Saltillo', 'Coahuila')
    ],
    'Russia': [
        ('Moscow', 'Moscow'), ('Saint Petersburg', 'Saint Petersburg'), ('Novosibirsk', 'Novosibirsk Oblast'),
        ('Yekaterinburg', 'Sverdlovsk Oblast'), ('Nizhny Novgorod', 'Nizhny Novgorod Oblast'), ('Kazan', 'Republic of Tatarstan'),
        ('Chelyabinsk', 'Chelyabinsk Oblast'), ('Omsk', 'Omsk Oblast'), ('Samara', 'Samara Oblast'),
        ('Rostov-on-Don', 'Rostov Oblast'), ('Ufa', 'Republic of Bashkortostan'), ('Krasnoyarsk', 'Krasnoyarsk Krai'),
        ('Perm', 'Perm Krai'), ('Voronezh', 'Voronezh Oblast'), ('Volgograd', 'Volgograd Oblast')
    ],
    'South Korea': [
        ('Seoul', 'Seoul'), ('Busan', 'Busan'), ('Incheon', 'Incheon'),
        ('Daegu', 'Daegu'), ('Daejeon', 'Daejeon'), ('Gwangju', 'Gwangju'),
        ('Ulsan', 'Ulsan'), ('Suwon', 'Gyeonggi'), ('Changwon', 'South Gyeongsang'),
        ('Goyang', 'Gyeonggi'), ('Yongin', 'Gyeonggi'), ('Seongnam', 'Gyeonggi')
    ],
    'Turkey': [
        ('Istanbul', 'Istanbul'), ('Ankara', 'Ankara'), ('Izmir', 'Izmir'),
        ('Bursa', 'Bursa'), ('Adana', 'Adana'), ('Gaziantep', 'Gaziantep'),
        ('Konya', 'Konya'), ('Antalya', 'Antalya'), ('Kayseri', 'Kayseri'),
        ('Mersin', 'Mersin'), ('Eskişehir', 'Eskişehir'), ('Diyarbakır', 'Diyarbakır')
    ],
    'Pakistan': [
        ('Karachi', 'Sindh'),('Hyderabad', 'Sindh'),('Sukkur', 'Sindh'),('Larkana', 'Sindh'),('Jamshoro', 'Sindh'),
        ('Umerkot', 'Sindh'), ('Lahore', 'Punjab'), ('Multan', 'Punjab'), ('Faisalabad', 'Punjab'), ('Islamabad', 'Punjab'),
        ('Bahawalpur', 'Punjab'), ('Quetta', 'Balochistan'), ('Khuzdar', 'Balochistan'), ('Gwadar', 'Balochistan'), ('Ziarat', 'Balochistan'),
        ('Uch', 'Balochistan'), ('Zin', 'Balochistan'), ('Gidani', 'Balochistan'), ('Hub', 'Balochistan'), ('Peshawar', 'KPK'),
        ('Swat', 'KPK'),  ('Mingora', 'KPK'), ('Fiza Gat', 'KPK'), ('Mitiari', 'KPK'), ('Kalaam', 'KPK'), ('Abbottabad', 'KPK'),
        ('Mahnsera', 'KPK'), ('Balakot', 'KPK'),  ('Muzaffarabad', 'AJK'),('Kutton', 'AJK'),('Rawalakot', 'AJK'),('Bagh', 'AJK'),
        ('Keran', 'AJK'), ('Shangla', 'AJK')
    ]
}

# Apparel and Cosmetics Brands (1000+ brands)
VENDOR_BRANDS = {
    'Apparel': [
        # Luxury Fashion
        'Louis Vuitton', 'Gucci', 'Hermès', 'Prada', 'Chanel', 'Versace', 'Armani', 'Dolce & Gabbana',
        'Balenciaga', 'Saint Laurent', 'Bottega Veneta', 'Fendi', 'Givenchy', 'Valentino', 'Tom Ford',
        'Alexander McQueen', 'Stella McCartney', 'Celine', 'Loewe', 'Moschino', 'Marni', 'Moncler',
        
        # Premium Fashion
        'Burberry', 'Ralph Lauren', 'Calvin Klein', 'Tommy Hilfiger', 'Hugo Boss', 'Armani Exchange',
        'Michael Kors', 'Kate Spade', 'Coach', 'Tory Burch', 'Marc Jacobs', 'Diane von Furstenberg',
        'Theory', 'J.Crew', 'Banana Republic', 'Brooks Brothers', 'Ted Baker', 'COS', 'Acne Studios',
        
        # Sportswear
        'Nike', 'Adidas', 'Puma', 'Reebok', 'Under Armour', 'New Balance', 'ASICS', 'Converse',
        'Vans', 'Fila', 'Champion', 'Kappa', 'Umbro', 'Mizuno', 'Saucony', 'Jordan', 'Yeezy',
        
        # Fast Fashion
        'Zara', 'H&M', 'Uniqlo', 'Forever 21', 'Topshop', 'Mango', 'Primark', 'ASOS', 'Boohoo',
        'Missguided', 'PrettyLittleThing', 'Shein', 'Romwe', 'Zaful', 'Fashion Nova', 'Urban Outfitters',
        
        # Department Store Brands
        'Nordstrom', 'Macy\'s', 'Bloomingdale\'s', 'Saks Fifth Avenue', 'Neiman Marcus', 'Barneys',
        'Talbots', 'Ann Taylor', 'Loft', 'White House Black Market', 'Chico\'s', 'Coldwater Creek',
        
        # Casual Wear
        'Gap', 'Old Navy', 'American Eagle', 'Hollister', 'Abercrombie & Fitch', 'Aeropostale',
        'Levi\'s', 'Wrangler', 'Lee', 'Dockers', 'Lands\' End', 'L.L.Bean', 'Patagonia', 'Columbia',
        
        # Denim Brands
        'True Religion', 'Citizens of Humanity', 'AG Jeans', '7 For All Mankind', 'Frame', 'Paige',
        'Mother', 'Hudson', 'Current/Elliott', 'Rag & Bone', 'Diesel', 'Replay', 'G-Star RAW',
        
        # Lingerie & Intimates
        'Victoria\'s Secret', 'Calvin Klein Underwear', 'La Perla', 'Agent Provocateur', 'Spanx',
        'Hanky Panky', 'Commando', 'Cosabella', 'Fleur du Mal', 'Journelle', 'ThirdLove', 'Savage X Fenty',
        
        # Swimwear
        'Speedo', 'Arena', 'TYR', 'Billabong', 'Rip Curl', 'Quiksilver', 'Roxy', 'Volcom',
        'Hurley', 'O\'Neill', 'Body Glove', 'Seafolly', 'Zimmermann', 'Melissa Odabash',
        
        # Footwear
        'Christian Louboutin', 'Jimmy Choo', 'Manolo Blahnik', 'Stuart Weitzman', 'Tory Burch',
        'Sam Edelman', 'Steve Madden', 'Nine West', 'Jessica Simpson', 'Michael Kors Shoes',
        'UGG', 'Timberland', 'Dr. Martens', 'Clarks', 'Sketchers', 'Crocs', 'Birkenstock',
        'Bata', 'Hush Puppies', 'Ecco', 'Geox', 'Naturalizer', 'Aerosoles', 'Easy Spirit',
        
        # Accessories
        'Ray-Ban', 'Oakley', 'Maui Jim', 'Warby Parker', 'Persol', 'Tom Ford Eyewear',
        'Fossil', 'Michael Kors Watches', 'Timex', 'Casio', 'Seiko', 'Citizen',
        'Longchamp', 'Kipling', 'Herschel', 'JanSport', 'Eastpak', 'Samsonite',
        
        # Ethnic/Regional Brands
        'Fabindia', 'W for Woman', 'Biba', 'Global Desi', 'Anita Dongre', 'Sabyasachi',
        'Manish Malhotra', 'Tarun Tahiliani', 'Rohit Bal', 'Ritu Kumar', 'Good Earth',
        'Ethnix', 'Aurelia', 'Rangmanch', 'Libas', 'Janasya', 'Varanga', 'Sangria',
        
        # Children\'s Wear
        'Carter\'s', 'OshKosh B\'gosh', 'Gymboree', 'The Children\'s Place', 'Crazy 8',
        'Janie and Jack', 'Tea Collection', 'Hanna Andersson', 'Mini Boden', 'Zutano',
        'Petit Bateau', 'Bonpoint', 'Stella McCartney Kids', 'Burberry Children',
        
        # Plus Size
        'Lane Bryant', 'Torrid', 'Ashley Stewart', 'Eloquii', 'Universal Standard',
        'ASOS Curve', 'Simply Be', 'Addition Elle', 'Catherines', 'Woman Within',
        
        # Maternity
        'Motherhood Maternity', 'A Pea in the Pod', 'Seraphine', 'Isabella Oliver',
        'ASOS Maternity', 'H&M Mama', 'Gap Maternity', 'Liz Lange', 'Ingrid & Isabel',
        
        # Workwear
        'Carhartt', 'Dickies', 'Red Wing', 'Caterpillar', 'Wolverine', 'Georgia Boot',
        'Timberland PRO', 'KEEN Utility', 'Ariat', 'Wrangler Workwear', 'Duluth Trading',
        
        # Outdoor/Active
        'The North Face', 'REI', 'Arc\'teryx', 'Outdoor Research', 'Marmot', 'Mountain Hardwear',
        'Salomon', 'Merrell', 'Keen', 'Vasque', 'Danner', 'Mammut', 'Fjällräven'
    ],
    
    'Cosmetics': [
        # Luxury Beauty
        'Chanel Beauty', 'Dior', 'YSL Beauty', 'Tom Ford Beauty', 'Giorgio Armani Beauty',
        'Guerlain', 'Lancôme', 'Estée Lauder', 'Clinique', 'SK-II', 'La Mer', 'La Prairie',
        'Sisley', 'Clarins', 'Shiseido', 'Clé de Peau Beauté', 'Sulwhasoo', 'Tatcha',
        
        # Mid-Range Beauty
        'MAC Cosmetics', 'Urban Decay', 'Too Faced', 'Benefit Cosmetics', 'Sephora Collection',
        'NARS', 'Bare Minerals', 'Smashbox', 'Tarte', 'IT Cosmetics', 'Drunk Elephant',
        'The Ordinary', 'Paula\'s Choice', 'Sunday Riley', 'Glossier', 'Fenty Beauty',
        
        # Drugstore Beauty
        'L\'Oréal Paris', 'Maybelline', 'Revlon', 'CoverGirl', 'Neutrogena', 'Olay',
        'Garnier', 'Pond\'s', 'Nivea', 'Dove', 'Aveeno', 'Cetaphil', 'Simple',
        'Clean & Clear', 'St. Ives', 'Burt\'s Bees', 'Yes To', 'Freeman',
        
        # Hair Care
        'Head & Shoulders', 'Pantene', 'Herbal Essences', 'TRESemmé', 'Sunsilk', 'Dove Hair',
        'Matrix', 'Redken', 'Paul Mitchell', 'Schwarzkopf', 'Wella', 'Joico', 'Kerastase',
        'Moroccanoil', 'Olaplex', 'Living Proof', 'Ouai', 'Briogeo', 'DevaCurl', 'Shea Moisture',
        
        # Men\'s Grooming
        'Gillette', 'Old Spice', 'Axe', 'Dove Men+Care', 'Nivea Men', 'L\'Oréal Men Expert',
        'Kiehl\'s', 'Jack Black', 'Baxter of California', 'American Crew', 'The Art of Shaving',
        'Harry\'s', 'Dollar Shave Club', 'Cremo', 'Every Man Jack', 'Brickell Men\'s Products',
        
        # Natural/Organic Beauty
        'Tata Harper', 'Drunk Elephant', 'Youth to the People', 'Herbivore Botanicals',
        'Fresh', 'Origins', 'Aveda', 'The Body Shop', 'Lush', 'Korres', 'Caudalie',
        'Weleda', 'Dr. Hauschka', 'Eminence', 'Jurlique', 'REN Clean Skincare',
        
        # K-Beauty
        'Innisfree', 'Etude House', 'The Face Shop', 'Tony & Tina', 'Missha', 'It\'s Skin',
        'Banila Co', 'Cosrx', 'Dr. Jart+', 'Laneige', 'Amorepacific', 'Sulwhasoo',
        'Hera', 'Iope', 'The History of Whoo', 'Ohui', 'Su:m37', 'Primera',
        
        # Indian Beauty Brands
        'Lakme', 'Biotique', 'Himalaya', 'Patanjali', 'Shahnaz Husain', 'VLCC',
        'Lotus Herbals', 'Colorbar', 'Faces Canada', 'Elle 18', 'Blue Heaven',
        'Revlon India', 'Chambor', 'Coloressence', 'Insight Cosmetics', 'Miss Claire',
        
        # Niche/Indie Beauty
        'Glossier', 'Rare Beauty', 'Haus Labs', 'KKW Beauty', 'Kylie Cosmetics',
        'Jeffree Star Cosmetics', 'Morphe', 'Anastasia Beverly Hills', 'Huda Beauty',
        'Pat McGrath Labs', 'Charlotte Tilbury', 'Hourglass', 'Natasha Denona',
        
        # Skincare Specialists
        'CeraVe', 'La Roche-Posay', 'Vichy', 'Avène', 'Eucerin', 'Bioderma', 'Embryolisse',
        'Nuxe', 'Caudalie', 'Lierac', 'Filorga', 'Decléor', 'Clarins', 'Payot',
        
        # Fragrance Houses
        'Chanel Fragrance', 'Dior Fragrance', 'Tom Ford Fragrance', 'Creed', 'Scents & Secrets']}

product_types = [
    # Apparel
    "T-Shirts", "Shirts", "Blouses", "Tunics", "Sweaters", "Hoodies", "Jackets", "Coats", "Blazers", "Suits",
    "Jeans", "Trousers", "Pants", "Shorts", "Skirts", "Dresses", "Jumpsuits", "Leggings", "Tracksuits", "Overalls",
    "Sarees", "Kurtas", "Lehengas", "Abayas", "Cardigans", "Tank Tops", "Vests", "Shawls", "Scarves", "Dupattas",
    "Underwear", "Bras", "Panties", "Boxers", "Lingerie Sets", "Sleepwear", "Nightgowns", "Robes", "Swimwear", "Beachwear",
    
    # Footwear
    "Sneakers", "Running Shoes", "Sandals", "Flip Flops", "Heels", "Pumps", "Boots", "Loafers", "Slippers", "Formal Shoes",
    
    # Accessories
    "Watches", "Sunglasses", "Handbags", "Wallets", "Backpacks", "Belts", "Hats", "Caps", "Gloves", "Ties",

    # Cosmetics
    "Lipstick", "Lip Gloss", "Lip Balm", "Foundation", "Concealer", "Compact Powder", "Highlighter", "Blush", "Primer", "Setting Spray",
    "Mascara", "Eyeliner", "Eyeshadow", "Kohl", "Eyebrow Pencil", "Makeup Remover", "Nail Polish", "Nail Polish Remover", "Makeup Brushes", "Beauty Blender",
    
    # Skincare
    "Face Wash", "Face Scrub", "Moisturizer", "Sunscreen", "Face Mask", "Face Serum", "Night Cream", "Toner", "Acne Treatment", "Whitening Cream",

    # Haircare
    "Shampoo", "Conditioner", "Hair Oil", "Hair Mask", "Hair Serum", "Hair Gel", "Hair Spray", "Hair Color", "Dry Shampoo", "Hair Removal Cream",

    # Fragrances & Others
    "Perfume", "Deodorant", "Body Lotion", "Body Wash", "Hand Cream", "Foot Cream", "Shaving Cream", "Aftershave", "Beard Oil", "Face Razor"
]

def insert_producttypes(conn, products):
    cursor = conn.cursor()
    for product in products:
        try:
            cursor.execute("INSERT INTO item_types (name) VALUES (?);", (product,))
        except Exception as e:
            pass
    conn.commit()

def insert_countries(conn, countries):
    cursor = conn.cursor()
    for name, code in countries:
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO countries (name, country_code) VALUES (?, ?)", (name, code)
            )
        except Exception as e:
            print(f"Error inserting country {name}: {e}")
    conn.commit()

def insert_cities(conn, cities_data):
    cursor = conn.cursor()
    for country_name, cities in cities_data.items():
        cursor.execute("SELECT id FROM countries WHERE name = ?", (country_name,))
        country = cursor.fetchone()
        if not country:
            print(f"Country '{country_name}' not found in DB.")
            continue
        country_id = country[0]
        for city_name, state in cities:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO cities (name, country_id, state_province) VALUES (?, ?, ?)",
                    (city_name, country_id, state)
                )
            except Exception as e:
                print(f"Error inserting city {city_name}: {e}")
    conn.commit()

def insert_vendors(conn, vendors_dict):
    cursor = conn.cursor()
    for category, brands in vendors_dict.items():
        for brand in brands:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO vendors (name) VALUES (?)", (brand,)
                )
            except Exception as e:
                print(f"Error inserting vendor {brand}: {e}")
    conn.commit()

# ===============================
# MAIN
# ===============================

def main():
    conn = connect_db()
    insert_countries(conn, COUNTRIES_DATA)
    insert_cities(conn, CITIES_DATA)
    insert_vendors(conn, VENDOR_BRANDS)
    insert_producttypes(conn, product_types)
    conn.close()
    print("Data insertion complete.")

main()