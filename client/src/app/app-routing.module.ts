import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { ProductSearchComponent } from './product-search/product-search.component';
import { InventoryComponent } from './inventory/inventory.component';
import { AvailabilityComponent } from './availability/availability.component';

const routes: Routes = [
  {path: '', pathMatch: 'full', redirectTo: 'search'},
  {path: 'search' , component: ProductSearchComponent},
  {path: 'inventory', component: InventoryComponent},
  {path: 'availability', component: AvailabilityComponent}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {

}
