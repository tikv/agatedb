window.BENCHMARK_DATA = {
  "lastUpdate": 1657630173513,
  "repoUrl": "https://github.com/tikv/agatedb",
  "entries": {
    "Benchmark with RocksDB": [
      {
        "commit": {
          "author": {
            "email": "ganziheng98@gmail.com",
            "name": "Ziheng Gan",
            "username": "GanZiheng"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a4e06f13dbf1e985cf2fbe6decfcde1505e9feed",
          "message": "bench: add bench action (#173)\n\n* add bench action\r\n\r\nSigned-off-by: GanZiheng <ganziheng98@gmail.com>",
          "timestamp": "2022-07-12T19:33:54+08:00",
          "tree_id": "ed0990f9e0ba699ea1ef646d99c6867d3abac091",
          "url": "https://github.com/tikv/agatedb/commit/a4e06f13dbf1e985cf2fbe6decfcde1505e9feed"
        },
        "date": 1657630172608,
        "tool": "cargo",
        "benches": [
          {
            "name": "agate sequentially populate small value",
            "value": 1227404156,
            "range": "± 121968460",
            "unit": "ns/iter"
          },
          {
            "name": "agate randomly populate small value",
            "value": 1519528291,
            "range": "± 100731626",
            "unit": "ns/iter"
          },
          {
            "name": "agate randread small value",
            "value": 213488370,
            "range": "± 6247470",
            "unit": "ns/iter"
          },
          {
            "name": "agate iterate small value",
            "value": 68524306,
            "range": "± 2090241",
            "unit": "ns/iter"
          },
          {
            "name": "agate sequentially populate large value",
            "value": 4456228511,
            "range": "± 68457606",
            "unit": "ns/iter"
          },
          {
            "name": "agate randomly populate large value",
            "value": 4704831447,
            "range": "± 97475668",
            "unit": "ns/iter"
          },
          {
            "name": "agate randread large value",
            "value": 288015670,
            "range": "± 4216055",
            "unit": "ns/iter"
          },
          {
            "name": "agate iterate large value",
            "value": 131037933,
            "range": "± 1041869",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate small value",
            "value": 238624629,
            "range": "± 10883050",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate small value",
            "value": 299735274,
            "range": "± 10459213",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread small value",
            "value": 179060840,
            "range": "± 1679275",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate small value",
            "value": 9289941,
            "range": "± 226020",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value",
            "value": 8168392690,
            "range": "± 220291963",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate large value",
            "value": 7825572073,
            "range": "± 8908757644",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread large value",
            "value": 1004872617,
            "range": "± 642161784",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate large value",
            "value": 624890915,
            "range": "± 4482347",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}