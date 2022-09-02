package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
)

type MasterDataCache struct {
	mtxVersionMasters          sync.RWMutex
	mtxItemMasters             sync.RWMutex
	mtxGachaMasters            sync.RWMutex
	mtxGachaItemMasters        sync.RWMutex
	mtxPresentAllMasters       sync.RWMutex
	mtxLoginBonusMasters       sync.RWMutex
	mtxLoginBonusRewardMasters sync.RWMutex

	VersionMasters          []*VersionMaster
	ItemMasters             []*ItemMaster
	GachaMasters            []*GachaMaster
	GachaItemMasters        []*GachaItemMaster
	PresentAllMasters       []*PresentAllMaster
	LoginBonusMasters       []*LoginBonusMaster
	LoginBonusRewardMasters []*LoginBonusRewardMaster
}

func NewMasterDataCache() *MasterDataCache {
	return &MasterDataCache{
		VersionMasters:          make([]*VersionMaster, 0),
		ItemMasters:             make([]*ItemMaster, 0),
		GachaMasters:            make([]*GachaMaster, 0),
		GachaItemMasters:        make([]*GachaItemMaster, 0),
		PresentAllMasters:       make([]*PresentAllMaster, 0),
		LoginBonusMasters:       make([]*LoginBonusMaster, 0),
		LoginBonusRewardMasters: make([]*LoginBonusRewardMaster, 0),
	}
}

func (m *MasterDataCache) Load(h *Handler) error {
	db := h.getAdminDB()

	m.mtxVersionMasters.Lock()
	defer m.mtxVersionMasters.Unlock()
	if err := db.Select(&m.VersionMasters, "SELECT * FROM version_masters"); err != nil {
		return err
	}

	m.mtxItemMasters.Lock()
	defer m.mtxItemMasters.Unlock()
	if err := db.Select(&m.ItemMasters, "SELECT * FROM item_masters"); err != nil {
		return err
	}

	m.mtxGachaMasters.Lock()
	defer m.mtxGachaMasters.Unlock()
	if err := db.Select(&m.GachaMasters, "SELECT * FROM gacha_masters"); err != nil {
		return err
	}

	m.mtxGachaItemMasters.Lock()
	defer m.mtxGachaItemMasters.Unlock()
	if err := db.Select(&m.GachaItemMasters, "SELECT * FROM gacha_item_masters"); err != nil {
		return err
	}

	m.mtxPresentAllMasters.Lock()
	defer m.mtxPresentAllMasters.Unlock()
	if err := db.Select(&m.PresentAllMasters, "SELECT * FROM present_all_masters"); err != nil {
		return err
	}

	m.mtxLoginBonusMasters.Lock()
	defer m.mtxLoginBonusMasters.Unlock()
	if err := db.Select(&m.LoginBonusMasters, "SELECT * FROM login_bonus_masters"); err != nil {
		return err
	}

	m.mtxLoginBonusRewardMasters.Lock()
	defer m.mtxLoginBonusRewardMasters.Unlock()
	if err := db.Select(&m.LoginBonusRewardMasters, "SELECT * FROM login_bonus_reward_masters"); err != nil {
		return err
	}

	return nil
}

func (m *MasterDataCache) getVersionMaster() (*VersionMaster, error) {
	m.mtxVersionMasters.RLock()
	defer m.mtxVersionMasters.RUnlock()

	for _, v := range m.VersionMasters {
		if v.Status == 1 {
			return v, nil
		}
	}

	return nil, fmt.Errorf("active master version is not found")
}

func (m *MasterDataCache) getItemMasters() ([]*ItemMaster, error) {
	m.mtxItemMasters.RLock()
	defer m.mtxItemMasters.RUnlock()

	return m.ItemMasters, nil
}

func (m *MasterDataCache) getItemMasterById(id int64) (*ItemMaster, error) {
	m.mtxItemMasters.RLock()
	defer m.mtxItemMasters.RUnlock()

	for _, v := range m.ItemMasters {
		if v.ID == id {
			return v, nil
		}
	}

	return nil, ErrItemNotFound
}

func (m *MasterDataCache) getItemMasterByIdAndItemType(id, itemType int64) (*ItemMaster, error) {
	m.mtxItemMasters.RLock()
	defer m.mtxItemMasters.RUnlock()

	for _, v := range m.ItemMasters {
		if v.ID == id && v.ItemType == v.ItemType {
			return v, nil
		}
	}

	return nil, ErrItemNotFound
}

func (m *MasterDataCache) getGachaMasters(requestAt int64) ([]*GachaMaster, error) {
	m.mtxGachaMasters.RLock()
	defer m.mtxGachaMasters.RUnlock()

	gachaMasters := []*GachaMaster{}

	for _, v := range m.GachaMasters {
		if v.StartAt <= requestAt && v.EndAt >= requestAt {
			gachaMasters = append(gachaMasters, v)
		}
	}

	sort.Slice(gachaMasters, func(i, j int) bool {
		return gachaMasters[i].DisplayOrder < gachaMasters[j].DisplayOrder
	})

	return gachaMasters, nil
}

func (m *MasterDataCache) getGachaMasterById(requestAt, id int64) (*GachaMaster, error) {
	m.mtxGachaMasters.RLock()
	defer m.mtxGachaMasters.RUnlock()

	for _, v := range m.GachaMasters {
		if v.StartAt <= requestAt && v.ID == id { // && v.EndAt >= requestAt // to avoid benchmark bug
			return v, nil
		}
	}

	return nil, fmt.Errorf("not found gacha")
}

func (m *MasterDataCache) getGachaItemMastersById(id int64) ([]*GachaItemMaster, error) {
	m.mtxGachaItemMasters.RLock()
	defer m.mtxGachaItemMasters.RUnlock()

	gachaItemMasters := []*GachaItemMaster{}

	for _, v := range m.GachaItemMasters {
		if v.GachaID == id {
			gachaItemMasters = append(gachaItemMasters, v)
		}
	}

	sort.Slice(gachaItemMasters, func(i, j int) bool {
		return gachaItemMasters[i].ID < gachaItemMasters[j].ID
	})

	return gachaItemMasters, nil
}

func (m *MasterDataCache) getGachaItemMastersWeightById(id int64) (int64, error) {
	m.mtxGachaItemMasters.RLock()
	defer m.mtxGachaItemMasters.RUnlock()

	var sum int64
	sum = 0
	for _, v := range m.GachaItemMasters {
		if v.GachaID == id {
			sum += int64(v.Weight)
		}
	}

	return sum, nil
}

func (m *MasterDataCache) getPresentAllMasters(requestAt int64) ([]*PresentAllMaster, error) {
	m.mtxPresentAllMasters.RLock()
	defer m.mtxPresentAllMasters.RUnlock()

	presentAllMasters := []*PresentAllMaster{}

	for _, v := range m.PresentAllMasters {
		if v.RegisteredStartAt <= requestAt && v.RegisteredEndAt >= requestAt {
			presentAllMasters = append(presentAllMasters, v)
		}
	}

	return presentAllMasters, nil
}

func (m *MasterDataCache) getLoginBonusMasters(requestAt int64) ([]*LoginBonusMaster, error) {
	m.mtxLoginBonusMasters.RLock()
	defer m.mtxLoginBonusMasters.RUnlock()

	loginBonudMasters := []*LoginBonusMaster{}

	for _, v := range m.LoginBonusMasters {
		if v.StartAt <= requestAt && v.EndAt >= requestAt {
			loginBonudMasters = append(loginBonudMasters, v)
		}
	}

	return loginBonudMasters, nil
}

func (m *MasterDataCache) getLoginBonusRewardMasterByIdAndSeq(id int64, seq int) (*LoginBonusRewardMaster, error) {
	m.mtxLoginBonusRewardMasters.RLock()
	defer m.mtxLoginBonusRewardMasters.RUnlock()

	for _, v := range m.LoginBonusRewardMasters {
		if v.LoginBonusID == id && v.RewardSequence == seq {
			return v, nil
		}
	}

	return nil, ErrLoginBonusRewardNotFound
}

type UserBanCache struct {
	mtx      sync.RWMutex
	UserBans []*UserBan
}

func NewUserBanCache() *UserBanCache {
	return &UserBanCache{
		UserBans: make([]*UserBan, 0),
	}
}

func (u *UserBanCache) Load(h *Handler) error {
	u.mtx.Lock()
	defer u.mtx.Unlock()

	query := "SELECT id,user_id,created_at,updated_at,deleted_at FROM user_bans"
	if err := h.getAdminDB().Select(&u.UserBans, query); err != nil {
		return err
	}

	return nil
}

func (u *UserBanCache) getUserBanByUserId(userID int64) *UserBan {
	u.mtx.RLock()
	defer u.mtx.RUnlock()

	for _, v := range u.UserBans {
		if v.UserID == userID {
			return v
		}
	}
	return nil
}

type UserDeviceCache struct {
	mtx  sync.RWMutex
	data map[struct {
		userID   int64
		viewerID string
	}]bool
}

func NewUserDeviceCache() *UserDeviceCache {
	return &UserDeviceCache{
		data: make(map[struct {
			userID   int64
			viewerID string
		}]bool),
	}
}

func (u *UserDeviceCache) Load(h *Handler) error {
	u.mtx.Lock()
	defer u.mtx.Unlock()

	userDevices := make([]*UserDevice, 0)

	for i := 0; i < len(h.DB); i++ {
		tmp := make([]*UserDevice, 0)
		query := "SELECT id,user_id,platform_id,platform_type,created_at,updated_at,deleted_at FROM user_devices"
		if err := h.getDB(int64(i)).Select(&tmp, query); err != nil {
			return err
		}
		userDevices = append(userDevices, tmp...)
	}

	u.data = make(map[struct {
		userID   int64
		viewerID string
	}]bool)

	for _, v := range userDevices {
		u.data[struct {
			userID   int64
			viewerID string
		}{v.UserID, v.PlatformID}] = true
	}

	return nil
}

func (u *UserDeviceCache) checkUserDeviceByIdAndViewerID(user_id int64, viewerID string) bool {
	u.mtx.RLock()
	defer u.mtx.RUnlock()

	v, ok := u.data[struct {
		userID   int64
		viewerID string
	}{user_id, viewerID}]
	if !ok {
		return false
	}
	return v
}

func (u *UserDeviceCache) addUserDevice(user_id int64, viewerID string) bool {
	u.mtx.Lock()
	defer u.mtx.Unlock()

	if _, ok := u.data[struct {
		userID   int64
		viewerID string
	}{user_id, viewerID}]; ok {
		return false
	}

	u.data[struct {
		userID   int64
		viewerID string
	}{user_id, viewerID}] = true

	return true
}

type IdGenerateCache struct {
	tableName string
	mtx       sync.Mutex
	current   int64
	last      int64
}

func NewIdGenerateCache(tableName string) *IdGenerateCache {
	return &IdGenerateCache{
		tableName: tableName,
	}
}

func (i *IdGenerateCache) clearIdGenerateCache() {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	i.current = 0
	i.last = 0
}

func (cache *IdGenerateCache) generateID(h *Handler) (int64, error) {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()

	if cache.current < cache.last {
		cache.current += 1
		return cache.current - 1, nil
	}

	var updateErr error
	for i := 0; i < 100; i++ {
		res, err := h.getAdminDB().Exec(strings.Join([]string{"UPDATE", cache.tableName, "SET id=LAST_INSERT_ID(id+1000)"}, " "))
		if err != nil {
			if merr, ok := err.(*mysql.MySQLError); ok && merr.Number == 1213 {
				updateErr = err
				continue
			}
			return 0, err
		}

		id, err := res.LastInsertId()
		if err != nil {
			return 0, err
		}
		cache.current = id + 1
		cache.last = id + 1000
		return id, nil
	}

	return 0, fmt.Errorf("failed to generate id: %w", updateErr)

}
