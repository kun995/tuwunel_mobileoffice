# Tuwunel 1.5.1

March 6, 2026

### Security Fixes

- A security audit of SSO/OIDC released with 1.5.0 uncovered several issues. We strongly advise everyone using SSO/OIDC upgrade to this release. Users should also note that until MSC2454 is implemented (tracked by #314) accounts will have to set a password to access functionality protected by User Interactive Authentication (e.g. when removing devices). We are deeply grateful to @outfrost and @exodrifter for their effort and professionalism as security researchers.

- Case-sensitive comparisons in Room Access Control Lists were fixed by @velikopter (ruma/ruma#2358) (matrix-construct/ruma#3) (814cbc2f3).

### New Features & Enhancements

- New options for `identity_provider` configurations include: `trusted` allowing association of SSO accounts to existing matrix users (#252); `unique_id_fallbacks` to disable random-string users; `registration` to prevent registration through an IdP altogether; `check_cookie` for deployments that cannot use cookies.

- Thanks to @Enginecrafter77 password authorization flows can now be disabled by configuring `login_with_password = false`. Clients will hide the input boxes for username and password. This option is useful for an e.g. SSO-only server. (#336)

- Thanks to @Lymia users of btrfs will see reduced space usage if they configure the new option `rocksdb_allow_fallocate = false`. (#322) (PR also has links to more information)

- Instructions for how to configure the TURN server built into Livekit and several corrections were contributed by serial documentation author @winyadepla in (#285).

- Many users will appreciate substantial documentation by @alametti for configuring well-known and root domain delegation in (#352).

- Thank you @the-hazelnut for updating TURN and Matrix RTC documentation with ports to be forwarded for NAT. (#305) (#306)

- The `username` claim is now recognized when deciding the MXID during SSO account registration thanks to a suggestion by @aazf in (#287).

- The max limit for `/messages` was increased from 100 to 1000 by @dasha-uwu which should match the limit on Synapse but with far less of a performance hazard.

- @dasha-uwu properly optimized certain checked-math macros; other checked-math macros were also optimized for inlining.

- Concurrent batch requests can now be made to a notary server. The default concurrency is now two, and the size of the batches have been decreased by a third. This should reduce the time it takes to join large rooms.

- Optimization of functions which hurt performance for syncing user-presence were partially completed, though with marked improvement from before.

- Optimization of new state-resolution functionality added during Project Hydra took place. Along with additional optimization for auth-chain gathering, CPU use for large/complex rooms (so-called "bad rooms") has been greatly reduced.

### Bug Fixes

- Special thanks to @hatomist for fixing an error which changes a users's account-type when they set a password (#313). This impacted LDAP and some SSO users. We apologize for the inconvenience this may have caused.

- We appreciate effort by @Jeidnx for addressing various issues with SSO/OIDC Identity Provider configuration in (#281). Also noteworthy was the idea to derive the callback_url from other parameters by default rather than explicitly requiring it. Thanks to @Magnitaizer for reporting initially in (#276).

- Thanks @VlaDexa for fixing the missing output formatting for the oauth delete command. (#321)

- Thank you @risu729 for updating the default port number in the docker run command documentation. (#298)

- Thank you @Lamby777 for removing an errant `version` field in the docker-compose example. (299)

- Thank you @cornerot for updating the docker-compose with-traefik which still said Conduit instead of Tuwunel after all this time. (#308)

- Thank you @exodrifter for fixing errors and typos in the MatrixRTC documentation (#343) based on a report by @RhenCloud (#338).

- Thank you @wuyukai0403 for proofreading and fixing a typo in the troubleshooting document. (#312)

- A report by @BVollmerhaus lead to the reopening of (#240) to use Livekit/lk-jwt-service when federation is disabled. This was re-resolved by @dasha-uwu in (b79920a).

- Thanks to @Jeidnx for identifying a missing SSO redirect route in (#290) which was fixed in (matrix-construct/ruma@0130f6a).

- We appreciate the panic report by @Spaenny in #296 which occurred during SSL-related upgrades on the main branch. Fixed by @dasha-uwu (87faf81).

- Thanks to report (#302) by @data-niklas whitespace in the configured `client_secret_file` is now properly ignored thanks to @dasha-uwu (6f5ae17).

- After @Giwayume reported in (#303) that URL previews failed for some sites, an investigation by @dasha-uwu discovered Tuwunel's User-Agent header required some adjustment.

- @dasha-uwu refactored the Unix socket listener with main-branch testing by @VlaDexa (#310) and follow-up fixes in (488bd62).

- @jonathanmajh reported in (#315) and @wmstens simultaneously reported in (#318) that admin status was not granted to the server's first user when registering with SSO/OIDC. This was fixed by (e74186a).

- After a report by @tcyrus in (#328) that the RPM postinst script is not properly creating the tuwunel user. This was fixed by @x86pup in (5a55f84).

- Thank you @cloudrac3r for reporting in (#330) that events were being unnecessarily sent to some appservices. This was fixed by @dasha-uwu in (d073e17).

- Thanks to the report in (#331) by @BVollmerhaus the first registered user is not granted admin when originating from an appservice. Fixed by @dasha-uwu in (9dfba59).

- The report by @rexbron in (#337) discovered that some distributions set modest limits on threads per process. On many-core (32+) we may exceed these limits. The `RLIMIT_NPROC` is now raised (9e09162) to mitigate this.

- @x86pup set ManagedOOMPreference=avoid due to systemd not recognizing pressure-based deallocation with `madvise(2)` is not an out-of-memory condition.

- @dasha-uwu removed unnecessary added delays in the client endpoint for reporting.

- Server shutdown did not properly indicate offline status of the conduit user due to a recent regression, now fixed.

- @dasha-uwu fixed logic issues in the client `/members` query filter. These same logic errors were also found in Synapse and Dendrite.

- @dasha-uwu fixed the missing advertisement for `org.matrix.msc3827.stable` in client `/versions`.

- Custom profile fields were sometimes being double-escaped in responses to clients due to a JSON re-interpretation issue which is now fixed.

- @dasha-uwu fixed checks related to canonical aliases (0381547c5).

- @dasha-uwu relaxed the `encryption_enabled_by_default_for_room_type` "invite" option to not match all rooms.

- @x86pup fixed an issue with `display_name` and `avatar_url` omitted in `/joined_members` (fixed in our Ruma).

- Event processing of missing `prev_event`'s are no longer interrupted by an error from a sibling `prev_event`. This reduces CPU use by not repeating event processing before it would otherwise succeed.
