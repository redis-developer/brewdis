import { Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute } from "@angular/router";
import { StompService, StompConfig } from '@stomp/ng2-stompjs';
import { HttpClient } from '@angular/common/http';
import { MatTable } from '@angular/material';
import { SearchService } from '../search.service';
import {MatSort} from '@angular/material/sort';
import {MatTableDataSource} from '@angular/material/table';

export interface InventoryData {
  store: string;
  sku: string;
  description: string;
  id: string;
  name: string;
  availableToPromise: number;
  onHand: number;
  allocated: number;
  reserved: number;
  virtualHold: number;
  delta: number;
  time: string;
  level: string;
}

@Component({
  selector: 'app-inventory',
  templateUrl: './inventory.component.html',
  styleUrls: ['./inventory.component.css']
})
export class InventoryComponent implements OnInit {

  API_URL = '/api/';
  @ViewChild(MatTable, { static: true }) table: MatTable<any>;
  @ViewChild(MatSort, {static: true}) sort: MatSort;

  private stompService: StompService;
  dataSource = new MatTableDataSource();
  displayedColumns: string[] = ['store', 'sku', 'name', 'availableToPromise', 'onHand', 'allocated', 'reserved', 'virtualHold'];

  constructor(private http: HttpClient, private route: ActivatedRoute, private searchService: SearchService) { }

  ngOnInit() {
    let store: string;
    this.route.paramMap.subscribe(params => {
      store = params.get('store');
    });
    this.dataSource.sort = this.sort;
    this.searchService.inventory(store).subscribe((inventory: InventoryData[]) => this.dataSource.data = inventory);
    this.http.get(this.API_URL + 'config/stomp').subscribe((stomp: any) => this.connectStompService(stomp));
  }

  connectStompService(config: any) {
    const stompUrl = config.protocol + '://' + config.host + ':' + config.port + config.endpoint;
    const stompConfig: StompConfig = {
      url: stompUrl,
      headers: {
        login: '',
        passcode: ''
      },
      heartbeat_in: 0,
      heartbeat_out: 20000,
      reconnect_delay: 5000,
      debug: true
    };
    this.stompService = new StompService(stompConfig);
    this.stompService.subscribe(config.inventoryTopic).subscribe(update => this.updateRowData(JSON.parse(update.body)));
  }

  updateRowData(row_obj) {
    this.dataSource.data = this.dataSource.data.filter((value: InventoryData, key) => {
      if (value.id == row_obj.id) {
        value.availableToPromise = row_obj.availableToPromise;
        value.onHand = row_obj.onHand;
        value.allocated = row_obj.allocated;
        value.reserved = row_obj.reserved;
        value.virtualHold = row_obj.virtualHold;
        value.time = row_obj.time;
        value.delta = row_obj.delta;
        value.level = row_obj.level;
      }
      return true;
    });
    this.table.renderRows();
  }

  isRecent(time: string) {
    var updateTime = new Date(time);
    var currentTime = new Date();
    var duration = currentTime.valueOf() - updateTime.valueOf();
    return duration < 1000;
  }

}
