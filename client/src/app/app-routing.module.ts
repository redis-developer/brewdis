import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { CatalogComponent } from './catalog/catalog.component';
import { InventoryComponent } from './inventory/inventory.component';
import { AvailabilityComponent } from './availability/availability.component';

const routes: Routes = [
  {path: '', pathMatch: 'full', redirectTo: 'catalog'},
  {path: 'catalog' , component: CatalogComponent},
  {path: 'inventory', component: InventoryComponent},
  {path: 'inventory/:store', component: InventoryComponent},
  {path: 'availability', component: AvailabilityComponent},
  {path: 'availability/:sku', component: AvailabilityComponent}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {

}
