import requests
import sys
import io
import json
import geopandas as gpd
import pandas as pd
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

from collections import OrderedDict
from datetime import datetime, timedelta
from flask import Flask, make_response, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_restx import Api, Resource, fields


# ----- Initialization ----- #
app = Flask(__name__)
app.config['DEBUG'] = True
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JSON_SORT_KEYS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///Z5336691.db'
db = SQLAlchemy(app)
api = Api(app, version='1.0', title='API Doc', description='', doc='/')
suburb_csv = sys.argv[1] if len(sys.argv) == 3 else "georef-australia-state-suburb.csv"
au_csv = sys.argv[2] if len(sys.argv) == 3 else "au.csv"
suburb_dataset = pd.read_csv(suburb_csv, sep=";")
suburb = {}
for index, row in suburb_dataset.iterrows():
    suburb[row['Official Name Suburb']] = row['Geo Point']
city_dataset = pd.read_csv(au_csv)


# ----- DB Models ----- #


class Event(db.Model):

    # Incremental ID
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # Basic necessary information
    name = db.Column(db.String(255), nullable=False)
    date = db.Column(db.Date, nullable=False)
    start_time = db.Column(db.Time, nullable=False)
    end_time = db.Column(db.Time, nullable=False)
    # Location information
    street = db.Column(db.String(255))
    suburb = db.Column(db.String(255))
    state = db.Column(db.String(255))
    post_code = db.Column(db.String(255))
    # Description
    description = db.Column(db.Text)
    # Update time is calculated by the app
    updated_at = db.Column(db.TIMESTAMP, nullable=False)

    def __repr__(self):
        return "id:{0} name:{1}".format(self.id, self.name)


# ----- Input Models ----- #

location_model = api.model('Location', {
    'street': fields.String(description='The street address', default='215B Night Ave'),
    'suburb': fields.String(required=True, description='The suburb', default='Kensington'),
    'state': fields.String(required=True, description='The state', default='NSW'),
    'post-code': fields.String(description='The postal code', default='2033'),
})

event_input_model = api.model('EventInput', {
    'name': fields.String(required=True, description='The name of the event', default='New event'),
    'date': fields.Date(required=True, description='The date of the event (YYYY-MM-DD)', default='2023-04-01'),
    'from': fields.String(required=True, description='The start time of the event (HH:MM)', default='16:00'),
    'to': fields.String(required=True, description='The end time of the event (HH:MM)', default='20:00'),
    'location': fields.Nested(location_model, required=True, description='The location of the event'),
    'description': fields.String(description='The description of the event'),
})

location_model_patch = api.model('LocationPatch', {
    'street': fields.String(description='The street address', default='215B Night Ave'),
    'suburb': fields.String(description='The suburb', default='Kensington'),
    'state': fields.String(description='The state', default='NSW'),
    'post-code': fields.String(description='The postal code', default='2033'),
})

event_input_model_patch = api.model('EventInputPatch', {
    'name': fields.String(description='The name of the event', default='New event'),
    'date': fields.Date(description='The date of the event (YYYY-MM-DD)', default='2023-04-01'),
    'from': fields.String(description='The start time of the event (HH:MM)', default='16:00'),
    'to': fields.String(description='The end time of the event (HH:MM)', default='20:00'),
    'location': fields.Nested(location_model_patch, description='The location of the event'),
    'description': fields.String(description='The description of the event'),
})


# ----- Routes ----- #


@api.route("/events")
class CreateEventHandler(Resource):
    @api.doc(description='Add a new event to the calendar.')
    @api.expect(event_input_model, validate=True)
    def post(self):
        # Parse the request payload
        payload = request.json

        # Create a new Event object and set its properties
        event = Event()
        try:
            event.name = payload['name']
            event.date = datetime.strptime(payload['date'], '%Y-%m-%d').date()
            event.start_time = datetime.strptime(payload['from'], '%H:%M').time()
            event.end_time = datetime.strptime(payload['to'], '%H:%M').time()
            location = payload.get('location')
            if location:
                event.street = location.get('street')
                event.suburb = location.get('suburb')
                event.state = location.get('state')
                event.post_code = location.get('post-code')
                if event.suburb is None or event.state is None:
                    raise TypeError("'suburb' and 'state' cannot be empty.")
            event.description = payload.get('description')
            event.updated_at = datetime.now()
        except (KeyError, TypeError, ValueError) as e:
            # if there is a KeyError or TypeError when trying to access the request data, return an error message
            return {'message': 'Invalid input: {}'.format(str(e))}, 400
        except Exception as e:
            # if there is any other type of exception, return the error message
            return {'message': 'Unexpected error: {}'.format(str(e))}, 400

        error_msg = validate_event(event)
        if error_msg != "":
            return {'message': error_msg}, 400

        # Save the Event to the database
        db.session.add(event)
        db.session.commit()

        try:
            response = jsonify(EventResponse(event).response)
        except Exception as e:
            db.session.rollback()
            return {'message': 'Unexpected error: {}'.format(str(e))}, 400

        return make_response(response, 201)

    @api.doc(description='Retrieve the list of available events. All four parameters are optional with default values '
                         'being "order=+id", "page=1", and "size=10", filter="id,name". "page" and "size" are used for '
                         'pagination; "size" shows the number of events per page. "order" is a comma-separated string '
                         'value to sort the list based on the given criteria. The string consists of two parts: the '
                         'first part is a special character "+" or "-" where "+" indicates ordering ascendingly, and '
                         '"-" indicates ordering descendingly. The second part is an attribute name which is one of '
                         '{id, name, datetime}.',
             params={'order': 'order by', 'page': 'page number', 'size': 'number of events per page',
                     'filter': 'filter which fields to show'})
    def get(self):
        # Get query parameters
        raw_orders = request.args.get('order')
        orders = set(raw_orders.replace(' ', '+').split(',')) if raw_orders else {"+id"}
        if ('+id' in orders and '-id' in orders) or ('+name' in orders and '-name' in orders) or ('+datetime' in orders and '-datetime' in orders):
            return {'message': f'Invalid order: cannot order a field ascendingly and descendingly at the mean time.'}, 400
        raw_page = request.args.get('page')
        page = int(raw_page) if raw_page else 1
        raw_size = request.args.get('size')
        size = int(raw_size) if raw_size else 10
        raw_fields = request.args.get('filter')
        fields = set(raw_fields.split(',')) if raw_fields else {"id", "name"}
        total = len(Event.query.all())
        if total // size + 1 < page:
            return {'message': 'Page number exceeds.'}, 400

        # Validate query parameters
        allowed_fields = ['id', 'name', 'date', 'from', 'to', 'location']
        allowed_orders = ['+id', '-id', '+name', '-name', '+datetime', '-datetime']
        field_list = []
        for field in fields:
            if field not in allowed_fields:
                return {'message': f'Invalid filter: {field}'}, 400
            if field == 'from':
                field_list.append(Event.start_time)
            elif field == 'to':
                field_list.append(Event.end_time)
            elif field == 'location':
                location_fields = [Event.street, Event.suburb, Event.state, Event.post_code]
                if len(field_list) == 0:
                    field_list = location_fields
                else:
                    field_list.extend(location_fields)
            else:
                field_list.append(eval("{0}.{1}".format('Event', field)))
        order_list = []
        for order in orders:
            if order not in allowed_orders:
                return {'message': f'Invalid order: {order}'}, 400
            sign = order[0]
            field = order[1:]
            if field != 'datetime':
                order_list.append(
                    eval("Event.{0}.asc()".format(field)) if sign == '+' else eval("Event.{0}.desc()".format(field)))
            else:
                order_list.append(
                    eval("Event.date.concat(' ').concat(Event.from).asc()") if sign == '+' else eval("Event.date.concat(' ').concat(Event.from).desc()"))

        # Retrieve events from database
        events = Event.query.order_by(*order_list).with_entities(*field_list).paginate(page=page, per_page=size, error_out=True).items

        response_events = []
        for event in events:
            tmpdict = OrderedDict()
            fields = event._fields
            for field in fields:
                tmpdict[field] = str(eval("event.{0}".format(field)))
            response_events.append(tmpdict)

        orders_str = ",".join(orders)
        fields_str = ",".join(fields)
        href = f'/events?order={orders_str}&page={page}&size={size}&filter={fields_str}'
        rsp = {
            "page": page,
            "page-size": size,
            "events": [*response_events],
            "_links": {
                "self": {"href": href}
            }
        }
        if total // size + 1 >= page + 1:
            next_href = f'/events?order={orders_str}&page={page+1}&size={size}&filter={fields_str}'
            rsp["_links"]["next"] = {"href": next_href}
        return make_response(jsonify(rsp), 200)


@api.route('/events/<int:id>')
class EventByIDHandler(Resource):
    @api.doc(description='Retrieve an event by its ID.', params={'id': 'event ID'})
    def get(self, id):
        # fetch the event by its ID
        event = Event.query.filter_by(id=id).first()
        if not event:
            return {'message': f'Event with ID {id} not found.'}, 404

        # fetch the previous event (if it exists) based on the date and start time
        previous_event = Event.query.filter(
            (Event.date < event.date) | ((Event.date == event.date) & (event.start_time >= Event.end_time))
        ).order_by(
            Event.date.desc(), Event.end_time.desc()
        ).first()

        # fetch the next event (if it exists) based on the date and start time
        next_event = Event.query.filter(
            (Event.date > event.date) | ((Event.date == event.date) & (event.end_time <= Event.start_time))
        ).order_by(
            Event.date.asc(), Event.start_time.asc()
        ).first()

        response = OrderedDict()
        # Basic information
        response["id"] = event.id
        response["last-update"] = event.updated_at.strftime('%Y-%m-%d %H:%M:%S')
        response["name"] = event.name
        response["date"] = event.date.strftime('%Y-%m-%d')
        response["from"] = event.start_time.strftime('%H:%M')
        response["to"] = event.end_time.strftime('%H:%M')
        location = OrderedDict()
        location["street"] = event.street
        location["suburb"] = event.suburb
        location["post-code"] = event.post_code
        response["location"] = location
        response["description"] = event.description
        # Metadata
        timediff = (event.date - datetime.now().date()).days
        if 0 <= timediff <= 7:
            metadata = OrderedDict()
            if event.suburb in suburb:
                latlon = suburb[event.suburb]
            elif event.suburb + " (" + event.state + ")" in suburb:
                latlon = suburb[event.suburb + " (" + event.state + ")"]
            else:
                return {'message': 'Invalid event location. You may update or delete this event.'}, 400
            weather_forecast = get_weather_forecast(latlon[0], latlon[1], timediff)
            metadata.update(weather_forecast)
            holidays = get_public_holidays(event.date.year)
            if event.date.strftime('%Y-%m-%d') in holidays:
                metadata["holiday"] = holidays[event.date.strftime('%Y-%m-%d')]
            metadata["weekend"] = event.date.weekday() in [5, 6]
            response["_metadata"] = metadata
        # Links
        links = OrderedDict()
        links["self"] = {"href": "/events/{0}".format(event.id)}
        if previous_event:
            links["previous"] = {"href": "/events/{0}".format(previous_event.id)}
        if next_event:
            links["next"] = {"href": "/events/{0}".format(next_event.id)}
        response["_links"] = links

        return make_response(jsonify(response), 200)

    @api.doc(description='Delete an event by its ID.', params={'id': 'event ID'})
    def delete(self, id):
        if not isinstance(id, int):
            return {'message': f'ID {id} is not valid.'}, 400
        # Try to fetch the event with the given ID from the database
        event = Event.query.filter_by(id=id).first()
        if not event:
            return {'message': f'Event with ID {id} not found.'}, 404

        # Delete the event from the database
        db.session.delete(event)
        db.session.commit()

        # Return the ID of the deleted event
        response = {
            'message': f'The event with id {id} was removed from the database!',
            'id': id
        }
        return make_response(jsonify(response), 200)

    @api.doc(description='Update an event by its ID.', params={'id': 'event ID'})
    @api.expect(event_input_model_patch, validate=True)
    def patch(self, id):
        # Try to fetch the event with the given ID from the database
        event = Event.query.filter_by(id=id).first()
        if not event:
            return {'message': f'Event with ID {id} not found.'}, 404

        # Update the event fields based on the request data
        try:
            if 'name' in request.json:
                event.name = request.json['name']
            if 'date' in request.json:
                event.date = datetime.strptime(request.json['date'], '%Y-%m-%d').date()
            if 'from' in request.json:
                event.start_time = datetime.strptime(request.json['from'], '%H:%M').time()
            if 'to' in request.json:
                event.end_time = datetime.strptime(request.json['to'], '%H:%M').time()
            if 'location' in request.json:
                location = request.json['location']
                if 'street' in location:
                    event.street = location['street']
                if 'suburb' in location:
                    event.suburb = location['suburb']
                if 'state' in location:
                    event.state = location['state']
                if 'post-code' in location:
                    event.post_code = location['post-code']
            if 'description' in request.json:
                event.description = request.json['description']
            event.updated_at = datetime.now()
        except (KeyError, TypeError, ValueError) as e:
            # if there is a KeyError or TypeError when trying to access the request data, return an error message
            return {'message': 'Invalid input: {}'.format(str(e))}, 400
        except Exception as e:
            # if there is any other type of exception, return the error message
            return {'message': 'Unexpected error: {}'.format(str(e))}, 400

        # After patch, the event should still be valid
        error_msg = validate_event(event)
        if error_msg != "":
            return {'message': error_msg}, 400

        try:
            # Commit the changes to the database
            db.session.commit()
            response = jsonify(EventResponse(event).response)
        except Exception as e:
            db.session.rollback()
            return {'message': 'Unexpected error: {}'.format(str(e))}, 400

        return make_response(response, 200)


@api.route('/events/statistics')
class EventStatistics(Resource):
    @api.doc(description='Get the statistics of the existing Events. '
                         'This operation accepts a parameter called "format" which can be either "json" or "image", '
                         'resulting in the statistics to be returned in json or image format',
             params={'format': 'Return format of the statistics'})
    def get(self):
        format = request.args.get('format')
        if not format:
            format = "json"
        if format not in ['json', 'image']:
            return {'message': 'The format should either be "json" or "image"'}, 400

        # Get the current date
        current_date = datetime.now().date()

        # Get the total number of events
        total_events = Event.query.count()

        # Get the number of events in the current week
        start_of_week = current_date - timedelta(days=current_date.weekday())
        end_of_week = start_of_week + timedelta(days=7)
        events_this_week = Event.query.filter(Event.date.between(start_of_week, end_of_week)).count()

        # Get the number of events in the current month
        start_of_month = datetime(current_date.year, current_date.month, 1) - timedelta(days=1)
        end_of_month = datetime(current_date.year, current_date.month+1, 1) - timedelta(days=1)
        events_this_month = Event.query.filter(Event.date.between(start_of_month, end_of_month)).count()

        # Get the number of events per day for the current month
        events = Event.query.all()
        events_per_day = {e.date.isoformat(): 0 for e in events}
        for e in events:
            events_per_day[e.date.isoformat()] += 1

        # Format the statistics as a dictionary
        stats = {
            'total': total_events,
            'total-current-week': events_this_week,
            'total-current-month': events_this_month,
            'per-days': {k: events_per_day[k] for k in sorted(events_per_day)}
        }

        # Return json
        if format == "json":
            return make_response(jsonify(stats), 200)
        # Return image
        visualize_events_statistics(stats)
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png")
        buffer.seek(0)
        response = make_response(buffer.getvalue())
        response.headers["Content-Type"] = "image/png"
        return response


@api.route('/weather')
class EventStatistics(Resource):
    @api.doc(description="View Australia's weather forecast.", params={'date': 'date to show the weather forecast'})
    def get(self):
        date_str = request.args.get('date')
        if not date_str:
            return "Date parameter missing", 400
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return "Invalid date format. Please use YYYY-MM-DD format.", 400

        # print(date, datetime.now(), date - datetime.now())
        if dt - datetime.now() > timedelta(days=6) or dt - datetime.now() < timedelta(days=-1):
            return "Please input a future date within 7 days from now.", 400

        df = pd.read_csv("georef-australia-state-suburb.csv", sep=";")
        df = df[df['Geo Shape'].apply(lambda x: isinstance(x, str))]
        df['coordinates'] = df['Geo Shape'].apply(lambda x: json.loads(x))
        df['coordinates'] = df['coordinates'].apply(lambda x: x["coordinates"])
        df['coordinates'] = df['coordinates'].apply(lambda x: x[0] if len(x[0]) > 1 else x[0][0])
        df['coordinates'] = df['coordinates'].apply(lambda c: [f"{x[0]} {x[1]}" for x in c])
        df['coordinates'] = df['coordinates'].apply(lambda c: ",".join(c))
        df['coordinates'] = df['coordinates'].apply(lambda c: f"POLYGON (({c}))")
        df = df[df['coordinates'].apply(lambda x: x.find('[') == -1)]
        gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['coordinates']))
        cities = ['Sydney', 'Darwin', 'Broome', 'Perth', 'Adelaide', 'Cairns']
        cities_weather = city_dataset[city_dataset['city'].isin(cities)].loc[:, ['city', 'lat', 'lng']]
        cities_weather['coordinates'] = 'POINT (' + cities_weather['lng'].astype(str) + ' ' + cities_weather['lat'].astype(str) + ')'
        cities_gdf = gpd.GeoDataFrame(cities_weather, geometry=gpd.GeoSeries.from_wkt(cities_weather['coordinates']))

        fig, ax = plt.subplots(figsize=(8, 8))
        ax.set_aspect('equal')
        ax.axis('off')
        ax.set_title(f"Weather forecast for Australia on {dt}")

        base = gdf.plot(cmap="OrRd", legend=True, ax=ax)
        cities_gdf.plot(ax=base, marker='o', color='blue', markersize=5)
        for city in cities:
            row = cities_weather[cities_weather['city'] == city]
            lat = row['lat'].iloc[0]
            lng = row['lng'].iloc[0]
            weather = get_weather_forecast(lat, lng, (dt - datetime.now()).days+1)
            ax.annotate(city + '\n' + weather['weather'] + ' ' + weather['temperature'], xy=(lng, lat), xytext=(lng-1, lat+1))
        # Save the figure to a bytes buffer and return it as a Flask response
        buffer = io.BytesIO()
        plt.savefig(buffer, format="png")
        buffer.seek(0)
        response = make_response(buffer.getvalue())
        response.headers["Content-Type"] = "image/png"

        return response


# ----- Responses ----- #

class EventResponse:
    def __init__(self, event):
        self.response = OrderedDict()
        self.response["id"] = event.id
        self.response["last-update"] = event.updated_at.strftime('%Y-%m-%d %H:%M:%S')
        self.response["links"] = {"self": {"href": "/events/{0}".format(event.id)}}


# ----- Helper Functions ----- #

def validate_event(event):
    error_msg = ""
    # Check if the event start time is later than end time
    if event.start_time >= event.end_time:
        error_msg = f'The event start time {event.start_time.strftime("%H:%M")} is not earlier than the end time {event.end_time.strftime("%H:%M")}.'
    # Check if the event overlaps with any existing events
    existing_events = Event.query.filter(
        Event.id != event.id,
        Event.date == event.date,
        (((event.start_time >= Event.start_time) & (event.start_time < Event.end_time)) |
         ((Event.start_time < event.end_time) & (Event.end_time >= event.end_time)))
    ).all()
    if existing_events:
        ids = ", ".join([str(e.id) for e in existing_events])
        error_msg = f'The event overlaps with existing events: {ids}.'
    # Check if the suburb is valid
    if event.state and event.state in ["nsw", "new south wales"]:  # special handle nsw according to FAQ
        event.state = "NSW"
    if event.suburb and event.state and event.suburb not in suburb and event.suburb + ' (' + event.state + ')' not in suburb:
        error_msg = f'The suburb {event.suburb} in state {event.state} does not exist.'
    return error_msg


def get_public_holidays(year):
    url = f"https://date.nager.at/api/v2/PublicHolidays/{year}/au"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve public holidays. Status code: {response.status_code}")
    holidays = {}
    for x in response.json():
        holidays[x['date']] = x['name']
    return holidays


def get_weather_forecast(lat, lon, timedelta):
    url = f'http://www.7timer.info/bin/api.pl?lon={lon}&lat={lat}&product=civil&output=json'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve weather forecast. Status code: {response.status_code}")
    complete_forecast = response.json()['dataseries']
    forecast = complete_forecast[7*timedelta]
    wind_speed = forecast['wind10m']['speed']
    if wind_speed == 1:
        wind_speed = "< 0.3m/s"
    elif wind_speed == 2:
        wind_speed = "0.3-3.4m/s"
    elif wind_speed == 3:
        wind_speed = "3.4-8.0m/s"
    elif wind_speed == 4:
        wind_speed = "8.0-10.8m/s"
    elif wind_speed == 5:
        wind_speed = "0.8-17.2m/s"
    elif wind_speed == 6:
        wind_speed = "17.2-24.5m/s"
    elif wind_speed == 7:
        wind_speed = "24.5-32.6m/s"
    elif wind_speed == 8:
        wind_speed = "> 32.6m/s"
    else:
        wind_speed = "Undefined"
    weather = forecast['weather']
    if weather == -9999:
        weather = "Undefined"
    elif weather.find('rainsnow') != -1:
        weather = "rain snow"
    elif weather.find('lightsnow') != -1:
        weather = "light snow"
    elif weather.find('lightrain') != -1:
        weather = "light rain"
    elif weather.find('rain') != -1:
        weather = "rain"
    elif weather.find('snow') != -1:
        weather = "snow"
    elif weather.find('shower') != -1:
        weather = "shower"
    elif weather.find('humid') != -1:
        weather = "humid"
    elif weather.find('cloudy') != -1:
        weather = "cloudy"
    elif weather.find('clear') != -1:
        weather = "clear"
    else:
        weather = "Undefined"
    humidity = forecast['rh2m']
    if humidity == -9999:
        humidity = "Undefined"
    temperature = forecast['temp2m']
    if temperature == -9999:
        temperature = "Undefined"
    else:
        temperature = str(temperature) + " C"
    return {'wind-speed': wind_speed, 'weather': weather, 'humidity': humidity, 'temperature': temperature}


def visualize_events_statistics(data):
    # create figure and axis objects
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))

    # plot bar chart for event per days
    dates = list(data["per-days"].keys())
    values = list(data["per-days"].values())
    ax1.bar(dates, values, color='C0')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Events per day')
    ax1.tick_params(axis='y', labelcolor='C0')
    ax1.tick_params(axis='x', rotation=45)

    # plot stacked bar chart for total events
    total = data["total"]
    total_week = data["total-current-week"]
    total_month = data["total-current-month"]
    current_week = [0, 0, total_week, 0, 0]
    current_month = [0, 0, total_month - total_week, 0, 0]
    other_month = [0, 0, total - total_month, 0, 0]
    x = [0, 1, 2, 3, 4]
    ax2.bar(x, current_week, color='C1', label='Current Week')
    ax2.bar(x, current_month, bottom=current_week, color='C2', label='Current Month')
    ax2.bar(x, other_month, bottom=[0, 0, total_month, 0, 0], color='C3', label='Other Month')
    ax2.set_ylim(0, total + 1)
    ax2.set_ylabel('Total Events')
    ax2.tick_params(axis='y', labelcolor='C1')

    # set legend
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(handles1 + handles2, labels1 + labels2, loc='upper left')

    # set title
    plt.title('Event Statistics')

    return fig


# ----- Main ----- #
if __name__ == '__main__':
    db.create_all()
    app.run()
