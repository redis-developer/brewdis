import { Component, OnInit } from '@angular/core';
import { SearchService } from '../search.service';
import { Observable } from 'rxjs';
import { FormControl } from '@angular/forms';
import { Router } from "@angular/router";

@Component({
  selector: 'app-product-search',
  templateUrl: './product-search.component.html',
  styleUrls: ['./product-search.component.css']
})
export class ProductSearchComponent implements OnInit {
  API_URL = '/api/';
  categories = [];
  styles = [];
  categoryField: FormControl;
  styleField: FormControl;
  selectedCategoryId = '';
  selectedStyleId = '';
  searchField: FormControl;
  results: Observable<any>;
  lat = 34.0030;
  lng = -118.4298;

  constructor(private router: Router, private searchService: SearchService) { }

  ngOnInit() {
    this.categoryField = new FormControl();
    this.categoryField.valueChanges.subscribe((category: any) => this.searchService.styles(this.selectedCategoryId).subscribe(data => this.styles = data));
    this.styleField = new FormControl();
    this.searchField = new FormControl();
    this.searchService.categories().subscribe(data => this.categories = data);
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition((position) => {
        this.lat = position.coords.latitude;
        this.lng = position.coords.longitude;
      });
    } else {
      alert("Geolocation is not supported by this browser.");
    }

  }

  search() {
    this.results = this.searchService.productSearch(this.selectedCategoryId, this.selectedStyleId, this.searchField.value, this.lng, this.lat);
  }
}
