// Aradia Time — Service Worker for Push Notifications
self.addEventListener("push", function(event) {
  var data = { title: "Aradia Time", body: "You have a new notification.", url: "/" };
  try {
    if (event.data) data = Object.assign(data, event.data.json());
  } catch (e) {}

  event.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: "/logo.png",
      badge: "/logo.png",
      data: { url: data.url || "/" }
    })
  );
});

self.addEventListener("notificationclick", function(event) {
  event.notification.close();
  var url = (event.notification.data && event.notification.data.url) || "/";
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then(function(clientList) {
      for (var i = 0; i < clientList.length; i++) {
        if (clientList[i].url.indexOf(self.location.origin) !== -1 && "focus" in clientList[i]) {
          clientList[i].postMessage({ type: "PUSH_NAV", url: url });
          return clientList[i].focus();
        }
      }
      return clients.openWindow(url);
    })
  );
});

self.addEventListener("install", function() { self.skipWaiting(); });
self.addEventListener("activate", function(event) { event.waitUntil(clients.claim()); });
