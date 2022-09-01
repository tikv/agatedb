window.BENCHMARK_DATA = {
  "lastUpdate": 1662015303856,
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
      },
      {
        "commit": {
          "author": {
            "email": "zhangjinpeng@pingcap.com",
            "name": "zhangjinpeng1987",
            "username": "zhangjinpeng1987"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "475e17bce85cf5985161004c068616a594923365",
          "message": "Opt: has option switch key value separation off (#182)\n\n* opt: has option turn off key value separation\r\n\r\nSigned-off-by: zhangjinpeng1987 <zhangjinpeng@pingcap.com>",
          "timestamp": "2022-07-13T08:38:33+08:00",
          "tree_id": "54c17d923d862b5e09e544fee453e21ae6838b74",
          "url": "https://github.com/tikv/agatedb/commit/475e17bce85cf5985161004c068616a594923365"
        },
        "date": 1657676930610,
        "tool": "cargo",
        "benches": [
          {
            "name": "agate sequentially populate small value",
            "value": 950760016,
            "range": "± 93707883",
            "unit": "ns/iter"
          },
          {
            "name": "agate randomly populate small value",
            "value": 1184869837,
            "range": "± 85487197",
            "unit": "ns/iter"
          },
          {
            "name": "agate randread small value",
            "value": 177670910,
            "range": "± 5057388",
            "unit": "ns/iter"
          },
          {
            "name": "agate iterate small value",
            "value": 52015625,
            "range": "± 4983056",
            "unit": "ns/iter"
          },
          {
            "name": "agate sequentially populate large value",
            "value": 4084853508,
            "range": "± 108040677",
            "unit": "ns/iter"
          },
          {
            "name": "agate randomly populate large value",
            "value": 4317690841,
            "range": "± 102079264",
            "unit": "ns/iter"
          },
          {
            "name": "agate randread large value",
            "value": 254972566,
            "range": "± 4933307",
            "unit": "ns/iter"
          },
          {
            "name": "agate iterate large value",
            "value": 123239452,
            "range": "± 1531873",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate small value",
            "value": 214600258,
            "range": "± 17597546",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate small value",
            "value": 258871068,
            "range": "± 22896260",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread small value",
            "value": 167619834,
            "range": "± 2221422",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate small value",
            "value": 9109342,
            "range": "± 199097",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value",
            "value": 8164651232,
            "range": "± 171814367",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate large value",
            "value": 7830978134,
            "range": "± 9162343965",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread large value",
            "value": 3137404174,
            "range": "± 32860992",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate large value",
            "value": 725217537,
            "range": "± 3369986",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "d8fbdefb3ee41dc848e226f931bacbc4e30ba6cd",
          "message": "docs: update readme about bench (#189)\n\n* update readme about bench\r\n\r\nSigned-off-by: GanZiheng <ganziheng98@gmail.com>",
          "timestamp": "2022-08-25T12:12:46+08:00",
          "tree_id": "3b6b196cb5cea677feacc8d631b0c446deef6cc6",
          "url": "https://github.com/tikv/agatedb/commit/d8fbdefb3ee41dc848e226f931bacbc4e30ba6cd"
        },
        "date": 1661405420849,
        "tool": "cargo",
        "benches": [
          {
            "name": "agate sequentially populate small value",
            "value": 767458715,
            "range": "± 157814989",
            "unit": "ns/iter"
          },
          {
            "name": "agate randomly populate small value",
            "value": 1016265173,
            "range": "± 37645279",
            "unit": "ns/iter"
          },
          {
            "name": "agate randread small value",
            "value": 198990403,
            "range": "± 4162191",
            "unit": "ns/iter"
          },
          {
            "name": "agate iterate small value",
            "value": 44787484,
            "range": "± 3759033",
            "unit": "ns/iter"
          },
          {
            "name": "agate sequentially populate large value",
            "value": 3992158171,
            "range": "± 96486285",
            "unit": "ns/iter"
          },
          {
            "name": "agate randomly populate large value",
            "value": 4383715051,
            "range": "± 121384547",
            "unit": "ns/iter"
          },
          {
            "name": "agate randread large value",
            "value": 310458527,
            "range": "± 6322958",
            "unit": "ns/iter"
          },
          {
            "name": "agate iterate large value",
            "value": 132538763,
            "range": "± 2714848",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate small value",
            "value": 196021031,
            "range": "± 10443330",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate small value",
            "value": 262360274,
            "range": "± 11131911",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread small value",
            "value": 175548697,
            "range": "± 2393984",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate small value",
            "value": 9408760,
            "range": "± 230330",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value",
            "value": 8151065389,
            "range": "± 232344724",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate large value",
            "value": 7820818505,
            "range": "± 9099966244",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread large value",
            "value": 3392804581,
            "range": "± 17746788",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate large value",
            "value": 759436400,
            "range": "± 4235841",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "43722125+wangnengjie@users.noreply.github.com",
            "name": "Panda",
            "username": "wangnengjie"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "070ff801ea782c503561b0f59276c62571dc2f74",
          "message": "max_version support (#190)\n\n* fix: max_version support\r\n\r\nSigned-off-by: wangnengjie <751614701@qq.com>",
          "timestamp": "2022-09-01T13:36:01+08:00",
          "tree_id": "3810a94484c704269888ecec822f6320357a5992",
          "url": "https://github.com/tikv/agatedb/commit/070ff801ea782c503561b0f59276c62571dc2f74"
        },
        "date": 1662015303387,
        "tool": "cargo",
        "benches": [
          {
            "name": "agate sequentially populate small value",
            "value": 928069066,
            "range": "± 28516280",
            "unit": "ns/iter"
          },
          {
            "name": "agate randomly populate small value",
            "value": 1246153615,
            "range": "± 30596314",
            "unit": "ns/iter"
          },
          {
            "name": "agate randread small value",
            "value": 241692670,
            "range": "± 5159963",
            "unit": "ns/iter"
          },
          {
            "name": "agate iterate small value",
            "value": 68984624,
            "range": "± 2348586",
            "unit": "ns/iter"
          },
          {
            "name": "agate sequentially populate large value",
            "value": 4696322892,
            "range": "± 76745179",
            "unit": "ns/iter"
          },
          {
            "name": "agate randomly populate large value",
            "value": 4995223601,
            "range": "± 51473343",
            "unit": "ns/iter"
          },
          {
            "name": "agate randread large value",
            "value": 345745841,
            "range": "± 7899335",
            "unit": "ns/iter"
          },
          {
            "name": "agate iterate large value",
            "value": 167279440,
            "range": "± 1641375",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate small value",
            "value": 208505099,
            "range": "± 3850728",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate small value",
            "value": 258250415,
            "range": "± 12656740",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread small value",
            "value": 161163473,
            "range": "± 1890300",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate small value",
            "value": 9299616,
            "range": "± 220167",
            "unit": "ns/iter"
          },
          {
            "name": "rocks sequentially populate large value",
            "value": 8203969721,
            "range": "± 124131609",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randomly populate large value",
            "value": 7869662849,
            "range": "± 8816183936",
            "unit": "ns/iter"
          },
          {
            "name": "rocks randread large value",
            "value": 3185053862,
            "range": "± 30726194",
            "unit": "ns/iter"
          },
          {
            "name": "rocks iterate large value",
            "value": 714119522,
            "range": "± 5083401",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}