import { useState } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './components/ui/tabs';
import { ShipmentFilters } from './components/ShipmentFilters';
import { ShipmentTable } from './components/ShipmentTable';
import { Planner } from './components/Planner';
import { Settings } from './components/Settings';

export default function App() {
  const [activeTab, setActiveTab] = useState('shipment');

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="px-3 py-4">
        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid w-full max-w-md grid-cols-3">
            <TabsTrigger value="planner">Планер</TabsTrigger>
            <TabsTrigger value="shipment">Отгрузка по складам</TabsTrigger>
            <TabsTrigger value="settings">Настройки</TabsTrigger>
          </TabsList>

          <TabsContent value="planner" className="mt-4">
            <Planner />
          </TabsContent>

          <TabsContent value="shipment" className="mt-4 space-y-4">
            <ShipmentFilters />
            <ShipmentTable />
          </TabsContent>

          <TabsContent value="settings" className="mt-4">
            <Settings />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
