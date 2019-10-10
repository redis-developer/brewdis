import { Component, OnInit } from '@angular/core';
import { StompService, StompConfig } from '@stomp/ng2-stompjs';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-inventory',
  templateUrl: './inventory.component.html',
  styleUrls: ['./inventory.component.css']
})
export class InventoryComponent implements OnInit {

  API_URL = '/api/';

  private stompService: StompService;
  inventory = [];

  constructor(private http: HttpClient) { }

  ngOnInit() {
    this.http.get(this.API_URL + 'inventory').subscribe((inventory: any) => this.inventory = inventory);
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
    this.stompService.subscribe(config.inventoryTopic).subscribe(update => this.inventory=JSON.parse(update.body));
  }

}
