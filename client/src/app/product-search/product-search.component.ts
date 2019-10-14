import { Component, OnInit } from '@angular/core';
import { HttpClient, HttpParams, HttpHeaders, HttpErrorResponse } from '@angular/common/http';
import { SearchService } from '../search.service';
import { Observable } from 'rxjs';
import { ReactiveFormsModule, FormControl, FormsModule } from '@angular/forms';
import { debounceTime } from 'rxjs/operators';

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

  constructor(private searchService: SearchService) { }

  ngOnInit() {
    this.categoryField = new FormControl();
    this.categoryField.valueChanges.subscribe((category: any)  => this.searchService.styles(this.selectedCategoryId).subscribe(data => this.styles = data));
    this.styleField = new FormControl();
    this.searchField = new FormControl();
    this.searchService.categories().subscribe(data => this.categories = data);
  }

  search() {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition((position) => {
          this.results = this.searchService.productSearch(position.coords.longitude, position.coords.latitude, this.selectedCategoryId, this.selectedStyleId, this.searchField.value);
        });
    } else {
      alert("Geolocation is not supported by this browser.");
    }
  }

}
