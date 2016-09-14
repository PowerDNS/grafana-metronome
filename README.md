# Grafana integration with PowerDNS Metronome

This repository provides integration between the [Grafana dashboard](http://grafana.org/) and [PowerDNS Metronome](https://github.com/ahupowerdns/metronome), allowing you to graph Metronome datasources in Grafana.

Integration is implemented using a custom Metronome loader plugin for the [Graphite-API server](http://graphite-api.readthedocs.io/en/latest/). This API server can be configured as a Graphite data source in Grafana to translate API requests to Metronome's REST API.

Additionally, it provides a ready-to-run [Docker Compose](https://docs.docker.com/compose/) file to quickly get Grafana and the customized Graphite-API server up and running with a simple `docker-compose up`.


## Quickstart using Docker

Prerequisites:

 * [Docker](https://docs.docker.com/engine/installation/)
 * [Docker Compose](https://docs.docker.com/compose/)

After cloning this repository, simply run:

    $ docker-compose up

You can add the `-d` flag to start the services in te background.

You will now have Grafana running on <http://localhost:13000/> with 'admin' as the default login and password.

After logging in as admin, click on the left-top menu > Data Sources > Add Data Source. Fill out the following fields:

 * Name: pick anything, like 'metronome'. You might want to click 'default' to make it the default data source.
 * Type: graphite
 * Url: http://graphiteapi:8000 (this hostname will be available within the Docker container)
 * Access: proxy
 * No authentication

Now you are ready to add new dashboards. You might want to check the [Grafana Getting Started Guide](http://docs.grafana.org/guides/gettingstarted/) on how to do this.


## Manual installation

If you want to setup Grafana and Graphite-API yourself without using Docker, you can follow the default documentation for both packages.

In order for Graphite-API to connect to Metronome servers, you need to copy the `graphite-api/metronome/` Python package to your Python site-packages (cleaner method coming soon), and add the following to your Graphite-API configuration file:

    finders:
      - metronome.MetronomeFinder
    metronome:
      # URL of your Metronome instance,
      # or use the public PowerDNS hosted server for testing
      url: https://metrics.powerdns.com/metronome
      # How often to update the list of Metronome metrics (in seconds)
      metrics_cache_expiry: 300

Grafana does not require any special configuration.

Even if you are not using Docker, you might want to look at the following files for inspiration on how to configure your setup and run Graphite-API:

 * `docker-compose.yml`
 * `graphite-api/Dockerfile`
 * `graphite-api/graphite-api.yaml`


## Debugging

Metronome API requests:

 * List of metrics: <https://metrics.powerdns.com/metronome?do=get-metrics&callback=_>
 * Fetch data: <https://metrics.powerdns.com/metronome?do=retrieve&callback=_&name=pdns.power4.auth.latency&begin=1473683300&end=1474690465&datapoints=100>

Graphite-API requests:

 * Query metrics: <http://localhost:18003/metrics/find?query=_pdns_view.auth.*>
 * Render graph: <http://localhost:18003/render?target=_pdns_view.auth.power4.auth.latency&from=-1h&until=now>
 * Get JSON data: <http://localhost:18003/render?target=_pdns_view.auth.power4.auth.latency&format=json&from=-1h&until=now>


## TODO

 * Distribute the Graphite-API metronome loader plugin as a proper Python package.
 * Include Grafana JSON dashboard files that provide similar graphs as Metronome does out of the box.

