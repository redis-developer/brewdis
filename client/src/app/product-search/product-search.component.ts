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
  selectedCategory = '';
  results: Observable<any>;
  styles = [];
  styleField: FormControl;
  searchField: FormControl;

  constructor(private http: HttpClient, private searchService: SearchService) { }

  ngOnInit() {
    this.searchField = new FormControl();
    this.styleField = new FormControl();
    this.styleField.valueChanges.pipe(
      debounceTime(300)
    ).subscribe(prefix => this.searchService.productStyles(this.styleField.value).subscribe(data => { this.styles = data; }));
    this.searchService.productCategories().subscribe(data => { this.categories = data; });
  }

  styleSelected(style: any) {
    this.searchField.setValue('@style:{' + style + '} ');
  }

  like(album: any) {
    this.searchService.likeAlbum(album);
    album.like = true;
  }

  displayFn(style: any) {
    if (style) { return style; }
  }

  search() {
    this.results = this.searchService.productSearch(this.selectedCategory, this.styleField.value, this.searchField.value);
	console.log(this.results);
  }

}
