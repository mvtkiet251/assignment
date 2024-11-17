# Import necessary libraries
from dataclasses import dataclass, field, fields
from iso3166 import countries
import json
import argparse
import pandas as pd
import requests

# Define a data class for hotel location
@dataclass
class Location:
    lat: float
    lng: float
    address: str
    city: str
    country: str

# Define a data class for hotel amenities
@dataclass
class Amenities:
    general: list[str]
    room: list[str]

# Define a data class for hotel images
@dataclass
class Images:
    rooms: list[str] = field(default_factory=list)
    site: list[str] = field(default_factory=list)
    amenities: list[str] = field(default_factory=list)

# Define a data class for hotels
@dataclass
class Hotel:
    id: str
    destination_id: str
    name: str
    location: Location
    description: str
    amenities: Amenities
    images: Images
    booking_conditions: list[str]

# Define a base class for suppliers
class BaseSupplier:
    def endpoint():
        """Define the endpoint URL to fetch supplier data"""

    def parse(obj: dict) -> Hotel:
        """Parse the supplier's data into a Hotel object"""

    def fetch(self):
        """Fetch data from the supplier's endpoint and parse it into Hotel objects"""
        url = self.endpoint()
        resp = requests.get(url)
        return [self.parse(dto) for dto in resp.json()]

# Modify amenities format for Acme supplier
def acme_amenities_modify(r):
    """Format and clean the amenities data from Acme supplier"""
    if len(r) <= 5:
        return r.strip().lower()
    ret = ''
    for i in range(len(r)):
        ret += r[i]
        if i + 1 < len(r) and r[i].islower() and r[i + 1].isupper():
            ret += ' '
    return ret.strip().lower()

# Define the Acme supplier class
class Acme(BaseSupplier):
    @staticmethod
    def endpoint():
        """Return the Acme supplier's API endpoint"""
        return 'https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/acme'

    @staticmethod
    def parse(dto: dict) -> Hotel:
        """Parse Acme supplier's data into a Hotel object"""
        return Hotel(
            id=dto['Id'],
            destination_id=str(dto['DestinationId']),
            name=dto['Name'],
            location=Location(
                lat=dto['Latitude'],
                lng=dto['Longitude'],
                address=dto['Address'],
                city=dto['City'],
                country=countries.get(dto['Country']).name
            ),
            description=dto['Description'],
            amenities=Amenities(
                general=[] if dto['Facilities'] is None else [acme_amenities_modify(r) for r in dto['Facilities']],
                room=[]
            ),
            images=Images(rooms=[], site=[], amenities=[]),
            booking_conditions=[]
        )

# Modify image data format for Paperflies supplier
def paperflies_images_modify(images):
    """Format and clean image data from Paperflies supplier"""
    return [
        {
            'link': data['link'],
            'description': data['caption']
        }
        for data in images
    ]

# Define the Paperflies supplier class
class Paperflies(BaseSupplier):
    @staticmethod
    def endpoint():
        """Return the Paperflies supplier's API endpoint"""
        return 'https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/paperflies'

    @staticmethod
    def parse(dto: dict) -> Hotel:
        """Parse Paperflies supplier's data into a Hotel object"""
        return Hotel(
            id=dto['hotel_id'],
            destination_id=str(dto['destination_id']),
            name=dto['hotel_name'],
            location=Location(
                lat=None,
                lng=None,
                address=dto['location']['address'],
                city=None,
                country=dto['location']['country']
            ),
            description=dto['details'],
            amenities=Amenities(
                general=dto['amenities']['general'],
                room=dto['amenities']['room']
            ),
            images=Images(**{key: paperflies_images_modify(value) for key, value in dto['images'].items()}),
            booking_conditions=dto['booking_conditions']
        )

# Modify image data format for Patagonia supplier
def patagonia_images_modify(images):
    """Format and clean image data from Patagonia supplier"""
    return [
        {
            'link': data['url'],
            'description': data['description']
        }
        for data in images
    ]

# Define the Patagonia supplier class
class Patagonia(BaseSupplier):
    @staticmethod
    def endpoint():
        """Return the Patagonia supplier's API endpoint"""
        return 'https://5f2be0b4ffc88500167b85a0.mockapi.io/suppliers/patagonia'

    @staticmethod
    def parse(dto: dict) -> Hotel:
        """Parse Patagonia supplier's data into a Hotel object"""
        return Hotel(
            id=dto['id'],
            destination_id=str(dto['destination']),
            name=dto['name'],
            location=Location(
                lat=dto['lat'],
                lng=dto['lng'],
                address=dto['address'],
                city=None,
                country=None
            ),
            description=dto['info'],
            amenities=Amenities(
                general=[],
                room=[] if dto['amenities'] is None else [r.strip().lower() for r in dto['amenities']]
            ),
            images=Images(**{key: patagonia_images_modify(value) for key, value in dto['images'].items()}),
            booking_conditions=[]
        )

# Find the longest content among a list of contents
def longest_content(content_list, default_value):
    """Return the longest content from a list, or a default value if empty"""
    if len(content_list) == 0:
        return default_value
    ret = None
    for i in content_list:
        if ret is None:
            ret = i
        content = str(i)
        if content == 'None' or content == '[]':
            content = ''
        if len(str(ret)) < len(content):
            ret = i
    return ret

# Service to manage and merge hotel data from multiple suppliers
class HotelsService:
    def __init__(self):
        """Initialize the HotelsService"""
        self.hotels = []

    def merge_amenities(self, df):
        """Merge amenities from multiple data sources"""
        ret = dict()
        for amenities_field in fields(Amenities):
            ret[amenities_field.name] = list(set(sum([i[amenities_field.name] if amenities_field.name in i.keys() else [] for i in df], [])))
        return ret

    def merge_images(self, df):
        """Merge images from multiple data sources"""
        ret = dict()
        for images_field in fields(Images):
            filtered = sum([i[images_field.name] if i is not None and images_field.name in i.keys() else [] for i in df], [])
            seen = set()
            ret[images_field.name] = []
            for element in filtered:
                if element['link'] not in seen:
                    seen.add(element['link'])
                    ret[images_field.name].append(element)
        return ret

    def merge_location(self, df, default_value):
        """Merge location data from multiple data sources"""
        ret = dict()
        for location_field in fields(Location):
            ret[location_field.name] = longest_content([i[location_field.name] if location_field.name in i.keys() else [] for i in df], default_value)
        return ret

    def merge_and_save(self, all_supplier_data):
        """Merge data from all suppliers and save the final dataset"""
        df = pd.DataFrame(all_supplier_data)
        hotel_ids = list(set([data.id for data in all_supplier_data]))

        for hotel_id in hotel_ids:
            cur_df = df.loc[df['id'] == hotel_id]
            hotel = dict()

            for hotel_field in fields(Hotel):
                if hotel_field.name == 'amenities':
                    hotel['amenities'] = self.merge_amenities(cur_df['amenities'])
                elif hotel_field.name == 'images':
                    hotel['images'] = self.merge_images(cur_df['images'])
                elif hotel_field.name == 'location':
                    hotel['location'] = self.merge_location(cur_df['location'], hotel_field.default)
                else:
                    hotel[hotel_field.name] = longest_content(cur_df[hotel_field.name], hotel_field.default)

            self.hotels.append(hotel)

        self.hotels = pd.DataFrame(self.hotels)
    
    def find(self, hotel_ids, destination_ids):
        """Find hotels based on hotel IDs and/or destination IDs"""
        hotel_list = hotel_ids.split(',') if hotel_ids != 'none' else []
        hotel_ids_rev = {value: pos for pos, value in enumerate(hotel_list)}
        
        destination_list = destination_ids.split(',') if destination_ids != 'none' else []
        destination_ids_rev = {value: pos for pos, value in enumerate(destination_list)}

        ret = self.hotels[
            (not hotel_list or self.hotels['id'].isin(hotel_list)) &
            (not destination_list or self.hotels['destination_id'].isin(destination_list))
        ]
        ret = json.loads(ret.to_json(orient='records'))

        if hotel_list != [] or destination_list != []:
            ret = sorted(ret, key=lambda x: (hotel_ids_rev.get(x['id'], 0), destination_ids_rev.get(x['destination_id'], 0)))

        return ret

# Fetch and filter hotels
def fetch_hotels(hotel_ids=None, destination_ids=None):
    """Fetch, merge, and filter hotel data from multiple suppliers"""
    suppliers = [
        Acme(),
        Paperflies(),
        Patagonia(),
    ]

    # Fetch data from all suppliers
    all_supplier_data = []
    for supp in suppliers:
        all_supplier_data.extend(supp.fetch())

    # Merge all the data and save it in-memory somewhere
    svc = HotelsService()
    svc.merge_and_save(all_supplier_data)

    # Fetch filtered data
    filtered = svc.find(hotel_ids, destination_ids)

    # Return as json
    return filtered
    
# Main execution function
def main():
    """Main function to parse arguments and fetch hotel data"""
    parser = argparse.ArgumentParser()
    
    parser.add_argument("hotel_ids", type=str, help="Hotel IDs")
    parser.add_argument("destination_ids", type=str, help="Destination IDs")
    
    # Parse the arguments
    args = parser.parse_args()
    
    hotel_ids = args.hotel_ids
    destination_ids = args.destination_ids
    
    result = fetch_hotels(hotel_ids, destination_ids)
    print(json.dumps(result, indent=2))

# Entry point of the script
if __name__ == "__main__":
    main()
